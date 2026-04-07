package solana

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"strings"
	"testing"

	"github.com/1hoodlab/go-indexing.git/internal/core"
)

// buildTransferEvent creates borsh bytes for a Transfer event: from(publicKey), amount(u64).
func buildTransferEvent(amount uint64) (discriminator []byte, payload []byte) {
	h := sha256.Sum256([]byte("event:Transfer"))
	disc := h[:8]
	buf := make([]byte, 32+8)                       // publicKey(32) + u64(8)
	binary.LittleEndian.PutUint64(buf[32:], amount) // amount
	return disc, buf
}

const transferIDL = `{
  "events": [
    {
      "name": "Transfer",
      "fields": [
        {"name": "from", "type": "publicKey"},
        {"name": "amount", "type": "u64"}
      ]
    }
  ],
  "types": []
}`

func TestParser_IDLPath(t *testing.T) {
	disc, payload := buildTransferEvent(9999)
	rawData := append(disc, payload...)

	raw := core.RawEvent{
		Address:  "TokenkegQ",
		EventKey: "TokenkegQ:" + strings.Repeat("00", 8),
		RawData:  rawData,
		Metadata: map[string]string{"idl": transferIDL},
	}

	p := NewEventParser()
	result, err := p.ParseEvent(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["amount"] != uint64(9999) {
		t.Errorf("expected amount=9999, got %v", result["amount"])
	}
	if result["_event_name"] != "Transfer" {
		t.Errorf("expected _event_name=Transfer, got %v", result["_event_name"])
	}
}

func TestParser_RawFallback_UnknownDiscriminator(t *testing.T) {
	rawData := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0xAA, 0xBB}
	raw := core.RawEvent{
		Address:  "SomeProg",
		EventKey: "SomeProg:0102030405060708",
		RawData:  rawData,
		Metadata: map[string]string{"idl": transferIDL}, // IDL exists but discriminator doesn't match.
	}

	p := NewEventParser()
	result, err := p.ParseEvent(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected raw fallback result")
	}
	if result["discriminator"] != "0102030405060708" {
		t.Errorf("expected discriminator in fallback, got %v", result["discriminator"])
	}
	if _, ok := result["data_base64"]; !ok {
		t.Error("expected data_base64 in fallback")
	}
}

func TestParser_RawFallback_NoIDL(t *testing.T) {
	rawData := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0x02, 0x03, 0xFF}
	raw := core.RawEvent{
		Address:  "SomeProg",
		EventKey: "SomeProg:deadbeef00010203",
		RawData:  rawData,
		Metadata: map[string]string{}, // no IDL.
	}

	p := NewEventParser()
	result, err := p.ParseEvent(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["discriminator"] != "deadbeef00010203" {
		t.Errorf("expected discriminator, got %v", result["discriminator"])
	}
}

func TestParser_EventKey_PassThrough(t *testing.T) {
	p := NewEventParser()
	key := "TokenkegQ:aabbccdd00112233"
	if got := p.EventKey(key); got != key {
		t.Errorf("EventKey should pass through: expected %q, got %q", key, got)
	}
}

func TestParser_IDL_Discriminator(t *testing.T) {
	// Verify that discriminator computation is SHA256("event:Transfer")[0:8].
	h := sha256.Sum256([]byte("event:Transfer"))
	expected := h[:8]

	var idl anchorIDL
	_ = json.Unmarshal([]byte(transferIDL), &idl)

	p := NewEventParser()
	disc := p.computeDiscriminator("Transfer")
	for i, b := range expected {
		if disc[i] != b {
			t.Errorf("discriminator byte %d: expected %02x, got %02x", i, b, disc[i])
		}
	}
}
