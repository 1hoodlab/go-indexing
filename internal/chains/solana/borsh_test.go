package solana

import (
	"encoding/binary"
	"math"
	"testing"
)

func makeIDLField(name, typ string) idlField {
	return idlField{Name: name, Type: typ}
}

func TestDecodeBorsh_Primitives(t *testing.T) {
	buf := make([]byte, 1+2+4+8+1)
	buf[0] = 42
	binary.LittleEndian.PutUint16(buf[1:], 1000)
	binary.LittleEndian.PutUint32(buf[3:], 70000)
	binary.LittleEndian.PutUint64(buf[7:], 1234567890)
	buf[15] = 1

	fields := []idlField{
		makeIDLField("a", "u8"),
		makeIDLField("b", "u16"),
		makeIDLField("c", "u32"),
		makeIDLField("d", "u64"),
		makeIDLField("e", "bool"),
	}

	result, err := decodeBorsh(buf, fields, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["a"] != uint64(42) {
		t.Errorf("u8: expected 42, got %v", result["a"])
	}
	if result["b"] != uint64(1000) {
		t.Errorf("u16: expected 1000, got %v", result["b"])
	}
	if result["c"] != uint64(70000) {
		t.Errorf("u32: expected 70000, got %v", result["c"])
	}
	if result["d"] != uint64(1234567890) {
		t.Errorf("u64: expected 1234567890, got %v", result["d"])
	}
	if result["e"] != true {
		t.Errorf("bool: expected true, got %v", result["e"])
	}
}

func TestDecodeBorsh_PublicKey(t *testing.T) {
	buf := make([]byte, 32)
	fields := []idlField{makeIDLField("pk", "publicKey")}
	result, err := decodeBorsh(buf, fields, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	pk, ok := result["pk"].(string)
	if !ok || pk == "" {
		t.Errorf("expected non-empty base58 string, got %v", result["pk"])
	}
}

func TestDecodeBorsh_String(t *testing.T) {
	text := "hello"
	buf := make([]byte, 4+len(text))
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(text)))
	copy(buf[4:], text)

	fields := []idlField{makeIDLField("msg", "string")}
	result, err := decodeBorsh(buf, fields, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["msg"] != "hello" {
		t.Errorf("expected 'hello', got %v", result["msg"])
	}
}

func TestDecodeBorsh_Vec(t *testing.T) {
	buf := make([]byte, 4+3*4)
	binary.LittleEndian.PutUint32(buf[:4], 3)
	binary.LittleEndian.PutUint32(buf[4:], 1)
	binary.LittleEndian.PutUint32(buf[8:], 2)
	binary.LittleEndian.PutUint32(buf[12:], 3)

	fields := []idlField{{Name: "nums", Type: map[string]interface{}{"vec": "u32"}}}
	result, err := decodeBorsh(buf, fields, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	nums, ok := result["nums"].([]interface{})
	if !ok || len(nums) != 3 {
		t.Fatalf("expected []interface{} len 3, got %T %v", result["nums"], result["nums"])
	}
	if nums[0] != uint64(1) || nums[1] != uint64(2) || nums[2] != uint64(3) {
		t.Errorf("unexpected values: %v", nums)
	}
}

func TestDecodeBorsh_OptionSome(t *testing.T) {
	buf := make([]byte, 1+8)
	buf[0] = 1
	binary.LittleEndian.PutUint64(buf[1:], 99)

	fields := []idlField{{Name: "val", Type: map[string]interface{}{"option": "u64"}}}
	result, err := decodeBorsh(buf, fields, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["val"] != uint64(99) {
		t.Errorf("expected 99, got %v", result["val"])
	}
}

func TestDecodeBorsh_OptionNone(t *testing.T) {
	buf := []byte{0}
	fields := []idlField{{Name: "val", Type: map[string]interface{}{"option": "u64"}}}
	result, err := decodeBorsh(buf, fields, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["val"] != nil {
		t.Errorf("expected nil for None, got %v", result["val"])
	}
}

func TestDecodeBorsh_InsufficientData(t *testing.T) {
	buf := []byte{1}
	fields := []idlField{makeIDLField("x", "u64")}
	_, err := decodeBorsh(buf, fields, nil)
	if err == nil {
		t.Fatal("expected error for insufficient data")
	}
}

func TestDecodeBorsh_F64(t *testing.T) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, math.Float64bits(3.14))
	fields := []idlField{makeIDLField("pi", "f64")}
	result, err := decodeBorsh(buf, fields, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result["pi"] != 3.14 {
		t.Errorf("expected 3.14, got %v", result["pi"])
	}
}
