package discovery

import (
	"encoding/json"
	"fmt"
	"net"
)

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

// SendAnnounce publishes a snapshot of entities. Send an empty/nil slice deliberately, rather
// than skipping the call, when a node has no entities yet - that still tells a listening peer
// this node exists, just with nothing to show.
func (s *Service) SendAnnounce(entities []EntityAnnouncement) error {
	body, err := json.Marshal(entities)
	if err != nil {
		return fmt.Errorf("discovery: unable to encode announce body: %w", err)
	}
	return s.send(Envelope{Type: MessageTypeAnnounce, Body: body})
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
