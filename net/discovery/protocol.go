package discovery

import (
	"encoding/json"
	"time"
)

// MessageType distinguishes the kinds of message exchanged over a Service's Transport.
type MessageType string

const (
	// MessageTypeQuery is sent on startup (and can be resent later) to ask "who's out there
	// with entities to share?" - lets a newly-started node discover peers that have been
	// running for a while without waiting for their next scheduled Announce.
	MessageTypeQuery MessageType = "query"

	// MessageTypeAnnounce carries a snapshot of the entities the sender currently knows
	// about. Sent both periodically and in direct response to a received MessageTypeQuery.
	MessageTypeAnnounce MessageType = "announce"

	// MessageTypeGoodbye is sent when a node is shutting down cleanly, so peers can drop its
	// entities immediately rather than waiting for them to go stale.
	MessageTypeGoodbye MessageType = "goodbye"
)

// Envelope is the JSON structure carried as a Transport Packet's Data.
type Envelope struct {
	Type MessageType `json:"type"`
	// Body holds a MessageType-specific payload - []EntityAnnouncement for
	// MessageTypeAnnounce, absent for MessageTypeQuery/MessageTypeGoodbye today. It's raw JSON
	// rather than a concrete type so a receiver can inspect Type before deciding whether (and
	// how) to decode it.
	Body json.RawMessage `json:"body,omitempty"`
}

// EntityAnnouncement describes one entity, as carried in a MessageTypeAnnounce Envelope's
// Body ([]EntityAnnouncement).
type EntityAnnouncement struct {
	// EntityId is stable and unique within the announcing node only. A receiver must
	// namespace it by Sender (see Event.Sender) before using it as a key anywhere entities
	// from multiple nodes might collide - two different nodes are free to reuse the same
	// EntityId independently.
	EntityId string `json:"entityId"`

	// EntityKind identifies the shape of State, e.g. "WirelessThermometer" or "Thermostat" -
	// matching the SPI entity type names pmaas-plugin-environment already uses for its own
	// entity registration, so a receiver can dispatch on it the same way.
	EntityKind string `json:"entityKind"`

	Name string `json:"name"`

	// State is the JSON encoding of the entity's current data (e.g. an
	// environmental.WirelessThermometer or environmental.Thermostat value) - opaque here so
	// this package stays independent of pmaas-spi/environment's types.
	State json.RawMessage `json:"state"`

	AsOf time.Time `json:"asOf"`
}
