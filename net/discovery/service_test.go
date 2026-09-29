package discovery

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBatchEntityAnnouncementsRespectsBudget(t *testing.T) {
	sender := NewInstanceID()
	maxBodyBytes, err := maxAnnounceBodyBytesForOverhead(frameOverheadForSender(sender))
	if err != nil {
		t.Fatalf("maxAnnounceBodyBytesForOverhead: %v", err)
	}

	// Build enough entities, each with a meaty State payload, that they can't possibly all fit
	// in one batch under maxAnnounceWireBytes - forcing batchEntityAnnouncements to actually
	// split, not just hand back everything in a single group.
	const entityCount = 40
	entities := make([]EntityAnnouncement, entityCount)
	for i := range entities {
		entities[i] = EntityAnnouncement{
			EntityId:   strings.Repeat("x", 20),
			EntityKind: "WirelessThermometer",
			Name:       "Test Device",
			State:      json.RawMessage(`{"Name":"Test Device","SensorData":{"Temperature":72.5,"HasHumidity":true,"Humidity":41.2}}`),
		}
	}

	batches, err := batchEntityAnnouncements(entities, maxBodyBytes)
	if err != nil {
		t.Fatalf("batchEntityAnnouncements: %v", err)
	}

	if len(batches) < 2 {
		t.Fatalf("expected multiple batches for %d entities under a %d-byte budget, got %d batch(es)",
			entityCount, maxBodyBytes, len(batches))
	}

	// Every entity must appear exactly once, across all batches, in original order.
	var seen []EntityAnnouncement
	for _, batch := range batches {
		seen = append(seen, batch...)
	}
	if len(seen) != entityCount {
		t.Fatalf("expected %d entities across all batches, got %d", entityCount, len(seen))
	}
	for i := range entities {
		if seen[i].EntityId != entities[i].EntityId {
			t.Fatalf("entity order/content not preserved at index %d", i)
		}
	}

	// Each batch's real marshaled length must match the budget accounting exactly, and the
	// FULL wire size (envelope -> frame), computed the same way Publish actually builds it,
	// must stay at or under maxAnnounceWireBytes - this is the guarantee the whole feature
	// exists to provide.
	for i, batch := range batches {
		bodyBytes, err := json.Marshal(batch)
		if err != nil {
			t.Fatalf("batch %d: marshal: %v", i, err)
		}
		if len(bodyBytes) > maxBodyBytes {
			t.Errorf("batch %d: body is %d bytes, over the %d-byte budget", i, len(bodyBytes), maxBodyBytes)
		}

		envelopeBytes, err := json.Marshal(Envelope{Type: MessageTypeAnnounce, Body: bodyBytes})
		if err != nil {
			t.Fatalf("batch %d: marshal envelope: %v", i, err)
		}

		frameBytes, err := encodeFrame(sender, envelopeBytes)
		if err != nil {
			t.Fatalf("batch %d: encode frame: %v", i, err)
		}

		if len(frameBytes) > maxAnnounceWireBytes {
			t.Errorf("batch %d: final wire size is %d bytes, over the %d-byte ceiling",
				i, len(frameBytes), maxAnnounceWireBytes)
		}
	}
}

func TestBatchEntityAnnouncementsEmptyInput(t *testing.T) {
	batches, err := batchEntityAnnouncements(nil, 1000)
	if err != nil {
		t.Fatalf("batchEntityAnnouncements: %v", err)
	}
	if len(batches) != 0 {
		t.Fatalf("expected no batches for no entities, got %d", len(batches))
	}
}

func TestBatchEntityAnnouncementsOversizedEntitySentAlone(t *testing.T) {
	oversized := EntityAnnouncement{
		EntityId: "oversized",
		State:    json.RawMessage(`"` + strings.Repeat("a", 2000) + `"`),
	}
	normal := EntityAnnouncement{EntityId: "normal", State: json.RawMessage(`"small"`)}

	// A tiny budget that can't possibly fit the oversized entity, to exercise the "can't split
	// it further, send it alone" fallback rather than dropping it or looping forever.
	batches, err := batchEntityAnnouncements([]EntityAnnouncement{oversized, normal}, 100)
	if err != nil {
		t.Fatalf("batchEntityAnnouncements: %v", err)
	}

	if len(batches) != 2 {
		t.Fatalf("expected the oversized entity in its own batch plus one for the normal entity, got %d batches", len(batches))
	}
	if len(batches[0]) != 1 || batches[0][0].EntityId != "oversized" {
		t.Fatalf("expected the oversized entity alone in the first batch, got %v", batches[0])
	}
	if len(batches[1]) != 1 || batches[1][0].EntityId != "normal" {
		t.Fatalf("expected the normal entity alone in the second batch, got %v", batches[1])
	}
}

func TestMaxAnnounceBodyBytesForOverheadRejectsImpossibleBudget(t *testing.T) {
	if _, err := maxAnnounceBodyBytesForOverhead(maxAnnounceWireBytes + 1); err == nil {
		t.Fatal("expected an error when frame overhead alone already exceeds the wire size ceiling")
	}
}
