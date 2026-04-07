package solana

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"

	"github.com/1hoodlab/go-indexing.git/internal/core"
)

// EventParser implements core.EventParser for Solana.
// It decodes Anchor IDL events via discriminator + borsh, with raw fallback.
type EventParser struct{}

// NewEventParser creates an EventParser.
func NewEventParser() *EventParser { return &EventParser{} }

// EventKey returns the identifier as-is.
// For Solana, EventKey is already canonical: "programAddress:discriminatorHex".
func (p *EventParser) EventKey(identifier string) string { return identifier }

// ParseEvent decodes a raw program event.
// If an IDL is available and the discriminator matches, uses borsh decoding.
// Otherwise falls back to returning the raw bytes as hex/base64.
func (p *EventParser) ParseEvent(raw core.RawEvent) (map[string]interface{}, error) {
	if len(raw.RawData) < 8 {
		return nil, fmt.Errorf("solana parser: event data too short (%d bytes)", len(raw.RawData))
	}

	discriminator := raw.RawData[:8]
	payload := raw.RawData[8:]

	// Attempt IDL-based decoding if IDL is present.
	idlJSON := raw.Metadata["idl"]
	if idlJSON != "" {
		result, err := p.decodeWithIDL(discriminator, payload, idlJSON)
		if err == nil && result != nil {
			return result, nil
		}
		// IDL decode failed (unknown discriminator or borsh error) -> fall through to raw.
	}

	return p.rawFallback(discriminator, raw.RawData), nil
}

// decodeWithIDL tries to find the event matching discriminator in the IDL and borsh-decode it.
// Returns nil, nil if the discriminator does not match any event.
func (p *EventParser) decodeWithIDL(discriminator, payload []byte, idlJSON string) (map[string]interface{}, error) {
	idl, err := parseIDL(idlJSON)
	if err != nil {
		return nil, err
	}
	typeMap := buildTypeMap(idl)

	for _, event := range idl.Events {
		expected := p.computeDiscriminator(event.Name)
		if !bytesEqual(discriminator, expected[:]) {
			continue
		}

		// Found matching event.
		fields, err := decodeBorsh(payload, event.Fields, typeMap)
		if err != nil {
			return nil, fmt.Errorf("borsh decode for event %q: %w", event.Name, err)
		}
		fields["_event_name"] = event.Name
		return fields, nil
	}

	return nil, nil // no matching event.
}

// computeDiscriminator returns sha256("event:<name>")[0:8].
func (p *EventParser) computeDiscriminator(eventName string) [8]byte {
	h := sha256.Sum256([]byte("event:" + eventName))
	var disc [8]byte
	copy(disc[:], h[:8])
	return disc
}

// rawFallback returns discriminator + base64 + hex for unknown events.
func (p *EventParser) rawFallback(discriminator, fullData []byte) map[string]interface{} {
	return map[string]interface{}{
		"discriminator": hex.EncodeToString(discriminator),
		"data_base64":   base64.StdEncoding.EncodeToString(fullData),
		"data_hex":      hex.EncodeToString(fullData),
	}
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
