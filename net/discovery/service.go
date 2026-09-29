package discovery

import (
	"encoding/json"
	"fmt"
	"net"
)

// maxAnnounceWireBytes is a conservative ceiling on the total wire size (post-envelope,
// post-frame) of a single Announce datagram - comfortably under the ~1472 bytes that typically
// survive a single Ethernet/Wi-Fi frame without IP fragmentation, which matters more now that
// this traffic can cross a routed hop (see discovery.Config.TTL) rather than staying on one
// switched segment. SendAnnounce splits a large entity list across multiple envelopes rather
// than risk sending anything IP-fragmented.
const maxAnnounceWireBytes = 1200

// Event is what Service delivers for each received Envelope, tagged with which peer sent it.
type Event struct {
	Sender InstanceID
	// From is the sender's source address, as seen by the OS on receipt - notably, its IP
	// survives that peer's process restarting (InstanceID is freshly random every time a
	// process starts), which makes it the more useful identity for a caller that wants to
	// namespace/deduplicate state per peer across restarts rather than per process instance.
	From     *net.UDPAddr
	Envelope Envelope
}

// Service adds JSON envelope encoding/decoding and typed send helpers on top of a raw
// Transport. It has no awareness of *why* entities are being announced or what a receiver
// should do with them - that's the calling plugin's job (see pmaas-plugin-environment's
// netdiscovery.go for a sketch). Service only owns getting Envelopes on and off the wire
// correctly.
type Service struct {
	transport *Transport
	eventCh   chan Event
	doneCh    chan struct{}
}

// NewService wraps transport, decoding every received Packet as an Envelope and re-publishing
// it on Events(). transport is owned by the returned Service from this point on - callers
// should use Service.Close, not close transport directly.
func NewService(transport *Transport) *Service {
	s := &Service{
		transport: transport,
		eventCh:   make(chan Event, 32),
		doneCh:    make(chan struct{}),
	}
	go s.run()
	return s
}

func (s *Service) run() {
	defer close(s.doneCh)
	defer close(s.eventCh)

	for packet := range s.transport.Packets() {
		var envelope Envelope
		if err := json.Unmarshal(packet.Data, &envelope); err != nil {
			fmt.Printf("discovery: dropping malformed envelope from %s: %v\n", packet.Sender, err)
			continue
		}

		select {
		case s.eventCh <- Event{Sender: packet.Sender, From: packet.From, Envelope: envelope}:
		default:
			fmt.Printf("discovery: dropping event from %s, receiver not keeping up\n", packet.Sender)
		}
	}
}

// Events returns the channel of decoded Envelopes from peers. Closed once Close has fully
// torn down the Service and its Transport.
func (s *Service) Events() <-chan Event {
	return s.eventCh
}

// SendQuery asks peers to announce what they have.
func (s *Service) SendQuery() error {
	return s.send(Envelope{Type: MessageTypeQuery})
}

// SendAnnounce publishes a snapshot of entities, split across as many Announce envelopes as
// needed to keep each one under maxAnnounceWireBytes on the wire (see batchEntityAnnouncements).
// Sends exactly one envelope, with an empty body, when entities is empty - deliberately, rather
// than skipping the call, since that still tells a listening peer this node exists, just with
// nothing to show.
func (s *Service) SendAnnounce(entities []EntityAnnouncement) error {
	maxBodyBytes, err := s.maxAnnounceBodyBytes()
	if err != nil {
		return err
	}

	batches, err := batchEntityAnnouncements(entities, maxBodyBytes)
	if err != nil {
		return err
	}

	if len(batches) == 0 {
		batches = [][]EntityAnnouncement{nil}
	}

	for _, batch := range batches {
		body, err := json.Marshal(batch)
		if err != nil {
			return fmt.Errorf("discovery: unable to encode announce batch: %w", err)
		}
		if err := s.send(Envelope{Type: MessageTypeAnnounce, Body: body}); err != nil {
			return err
		}
	}

	return nil
}

// maxAnnounceBodyBytes computes the largest raw entity-array JSON (`[...]`, brackets included)
// that's guaranteed to keep the final wire size (envelope, then frame) at or under
// maxAnnounceWireBytes. Both layers embed their contents as literal nested JSON (see frame),
// so this is just maxAnnounceWireBytes minus each layer's fixed structural overhead - no
// encoding-inflation math needed.
func (s *Service) maxAnnounceBodyBytes() (int, error) {
	return maxAnnounceBodyBytesForOverhead(s.transport.frameOverhead())
}

func maxAnnounceBodyBytesForOverhead(frameOverhead int) (int, error) {
	budgetForEnvelope := maxAnnounceWireBytes - frameOverhead
	if budgetForEnvelope <= 0 {
		return 0, fmt.Errorf(
			"discovery: max wire size %d is too small for this transport's frame overhead of %d bytes",
			maxAnnounceWireBytes, frameOverhead)
	}

	// Measure the envelope's own fixed structure (field names/quotes/braces plus the literal
	// "announce" type value) by marshaling it with a placeholder empty-array body and
	// subtracting that body's own 2 bytes ("[]") back out - Envelope's Body field is
	// omitempty, so marshaling with Body left nil would omit the "body" key entirely and
	// understate the overhead a real, non-empty body actually costs.
	probe, err := json.Marshal(Envelope{Type: MessageTypeAnnounce, Body: json.RawMessage("[]")})
	if err != nil {
		return 0, fmt.Errorf("discovery: unable to measure envelope overhead: %w", err)
	}
	fixedEnvelopeLen := len(probe) - 2

	maxBodyLen := budgetForEnvelope - fixedEnvelopeLen
	if maxBodyLen <= 0 {
		return 0, fmt.Errorf(
			"discovery: max wire size %d leaves no room for an announce body after envelope/frame overhead",
			maxAnnounceWireBytes)
	}

	return maxBodyLen, nil
}

// batchEntityAnnouncements greedily groups entities into batches whose JSON-array encoding
// (brackets, commas, and all) stays at or under maxBodyBytes, measuring each entity's own
// encoded length exactly once up front rather than repeatedly re-encoding a growing array.
// A single entity that doesn't fit under maxBodyBytes even alone is still sent alone, as a
// best-effort batch of one, since it can't be split any further.
func batchEntityAnnouncements(entities []EntityAnnouncement, maxBodyBytes int) ([][]EntityAnnouncement, error) {
	const brackets = 2 // "[" + "]"

	var batches [][]EntityAnnouncement
	var currentBatch []EntityAnnouncement
	currentLen := brackets

	for _, entity := range entities {
		encoded, err := json.Marshal(entity)
		if err != nil {
			return nil, fmt.Errorf("discovery: unable to measure entity %q for batching: %w", entity.EntityId, err)
		}

		addition := len(encoded)
		if len(currentBatch) > 0 {
			addition++ // comma separating it from the previous entry
		}

		if len(currentBatch) > 0 && currentLen+addition > maxBodyBytes {
			batches = append(batches, currentBatch)
			currentBatch = nil
			currentLen = brackets
			addition = len(encoded) // first entry of the new batch needs no comma
		}

		currentBatch = append(currentBatch, entity)
		currentLen += addition

		if currentLen > maxBodyBytes && len(currentBatch) == 1 {
			fmt.Printf(
				"discovery: entity %q is %d bytes, over the %d-byte announce budget on its own - sending it alone anyway\n",
				entity.EntityId, currentLen, maxBodyBytes)
		}
	}

	if len(currentBatch) > 0 {
		batches = append(batches, currentBatch)
	}

	return batches, nil
}

// SendGoodbye tells peers this node is shutting down cleanly.
func (s *Service) SendGoodbye() error {
	return s.send(Envelope{Type: MessageTypeGoodbye})
}

func (s *Service) send(envelope Envelope) error {
	data, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("discovery: unable to encode envelope: %w", err)
	}
	return s.transport.Publish(data)
}

// Close stops the Service and its underlying Transport, and waits for both to finish shutting
// down before returning.
func (s *Service) Close() error {
	err := s.transport.Close()
	<-s.doneCh
	return err
}
