package solana

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
)

// EventParser decodes Solana program events using Anchor IDL + borsh,
// with raw fallback for unknown discriminators or missing IDL.
type EventParser struct{}

func NewEventParser() *EventParser { return &EventParser{} }

// ParseEvent decodes base64 raw data into a map.
// Returns (eventName, decodedParams, error).
func (p *EventParser) ParseEvent(rawDataBase64, idlJSON string) (string, map[string]interface{}, error) {
	rawBytes, err := base64.StdEncoding.DecodeString(rawDataBase64)
	if err != nil {
		return "", nil, fmt.Errorf("base64 decode: %w", err)
	}
	if len(rawBytes) < 8 {
		return "", nil, fmt.Errorf("event data too short (%d bytes)", len(rawBytes))
	}

	discriminator := rawBytes[:8]
	payload := rawBytes[8:]

	if idlJSON != "" {
		eventName, result, err := p.decodeWithIDL(discriminator, payload, idlJSON)
		if err == nil && result != nil {
			return eventName, result, nil
		}
	}

	return "", p.rawFallback(discriminator, rawBytes), nil
}

func (p *EventParser) decodeWithIDL(discriminator, payload []byte, idlJSON string) (string, map[string]interface{}, error) {
	idl, err := parseIDL(idlJSON)
	if err != nil {
		return "", nil, err
	}
	typeMap := buildTypeMap(idl)

	for _, event := range idl.Events {
		expected := computeDiscriminatorBytes(event.Name)
		if !bytesEqual(discriminator, expected[:]) {
			continue
		}
		fields, err := decodeBorsh(payload, event.Fields, typeMap)
		if err != nil {
			return "", nil, fmt.Errorf("borsh decode %q: %w", event.Name, err)
		}
		return event.Name, fields, nil
	}

	return "", nil, nil
}

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
