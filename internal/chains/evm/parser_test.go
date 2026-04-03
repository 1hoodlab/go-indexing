package evm

import "testing"

func TestEventParser_EventKey(t *testing.T) {
	p := NewEventParser()
	key := p.EventKey("Transfer(address,address,uint256)")
	expected := "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	if key != expected {
		t.Fatalf("EventKey mismatch:\n  got:  %s\n  want: %s", key, expected)
	}
}

func TestEventParser_EventKeyConsistent(t *testing.T) {
	p := NewEventParser()
	key1 := p.EventKey("MemberAdded(address,uint8,uint256,uint256)")
	key2 := p.EventKey("MemberAdded(address,uint8,uint256,uint256)")
	if key1 != key2 {
		t.Fatal("EventKey not deterministic")
	}
	if key1 == "" {
		t.Fatal("EventKey returned empty string")
	}
}
