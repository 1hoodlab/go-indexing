package solana

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"

	solanago "github.com/gagliardetto/solana-go"
)

type idlField struct {
	Name string      `json:"name"`
	Type interface{} `json:"type"`
}

type idlEvent struct {
	Name   string     `json:"name"`
	Fields []idlField `json:"fields"`
}

type idlTypeDef struct {
	Name string `json:"name"`
	Type struct {
		Kind   string     `json:"kind"`
		Fields []idlField `json:"fields"`
	} `json:"type"`
}

type anchorIDL struct {
	Events []idlEvent   `json:"events"`
	Types  []idlTypeDef `json:"types"`
}

func parseIDL(idlJSON string) (*anchorIDL, error) {
	var idl anchorIDL
	if err := json.Unmarshal([]byte(idlJSON), &idl); err != nil {
		return nil, fmt.Errorf("parseIDL: %w", err)
	}
	return &idl, nil
}

func buildTypeMap(idl *anchorIDL) map[string]idlTypeDef {
	m := make(map[string]idlTypeDef, len(idl.Types))
	for _, t := range idl.Types {
		m[t.Name] = t
	}
	return m
}

// ComputeDiscriminator returns sha256("event:<name>")[0:8] as hex.
func ComputeDiscriminator(eventName string) string {
	h := sha256.Sum256([]byte("event:" + eventName))
	return hex.EncodeToString(h[:8])
}

func computeDiscriminatorBytes(eventName string) [8]byte {
	h := sha256.Sum256([]byte("event:" + eventName))
	var disc [8]byte
	copy(disc[:], h[:8])
	return disc
}

func decodeBorsh(data []byte, fields []idlField, types map[string]idlTypeDef) (map[string]interface{}, error) {
	result := make(map[string]interface{}, len(fields))
	offset := 0
	for _, field := range fields {
		val, n, err := decodeField(data[offset:], field.Type, types)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", field.Name, err)
		}
		result[field.Name] = val
		offset += n
	}
	return result, nil
}

func decodeField(data []byte, typ interface{}, types map[string]idlTypeDef) (interface{}, int, error) {
	switch t := typ.(type) {
	case string:
		return decodePrimitive(data, t, types)
	case map[string]interface{}:
		if inner, ok := t["vec"]; ok {
			return decodeVec(data, inner, types)
		}
		if inner, ok := t["option"]; ok {
			return decodeOption(data, inner, types)
		}
		if name, ok := t["defined"].(string); ok {
			return decodeDefined(data, name, types)
		}
		if inner, ok := t["array"]; ok {
			return decodeArray(data, inner, types)
		}
		return nil, 0, fmt.Errorf("unknown complex type: %v", t)
	default:
		return nil, 0, fmt.Errorf("unexpected type descriptor: %T %v", typ, typ)
	}
}

func decodePrimitive(data []byte, typ string, types map[string]idlTypeDef) (interface{}, int, error) {
	switch typ {
	case "bool":
		if len(data) < 1 {
			return nil, 0, fmt.Errorf("insufficient data for bool")
		}
		return data[0] != 0, 1, nil
	case "u8":
		if len(data) < 1 {
			return nil, 0, fmt.Errorf("insufficient data for u8")
		}
		return uint64(data[0]), 1, nil
	case "u16":
		if len(data) < 2 {
			return nil, 0, fmt.Errorf("insufficient data for u16")
		}
		return uint64(binary.LittleEndian.Uint16(data[:2])), 2, nil
	case "u32":
		if len(data) < 4 {
			return nil, 0, fmt.Errorf("insufficient data for u32")
		}
		return uint64(binary.LittleEndian.Uint32(data[:4])), 4, nil
	case "u64":
		if len(data) < 8 {
			return nil, 0, fmt.Errorf("insufficient data for u64")
		}
		return binary.LittleEndian.Uint64(data[:8]), 8, nil
	case "u128":
		if len(data) < 16 {
			return nil, 0, fmt.Errorf("insufficient data for u128")
		}
		lo := binary.LittleEndian.Uint64(data[:8])
		hi := binary.LittleEndian.Uint64(data[8:16])
		val := new(big.Int).SetUint64(hi)
		val.Lsh(val, 64)
		val.Or(val, new(big.Int).SetUint64(lo))
		return val.String(), 16, nil
	case "i8":
		if len(data) < 1 {
			return nil, 0, fmt.Errorf("insufficient data for i8")
		}
		return int64(int8(data[0])), 1, nil
	case "i16":
		if len(data) < 2 {
			return nil, 0, fmt.Errorf("insufficient data for i16")
		}
		return int64(int16(binary.LittleEndian.Uint16(data[:2]))), 2, nil
	case "i32":
		if len(data) < 4 {
			return nil, 0, fmt.Errorf("insufficient data for i32")
		}
		return int64(int32(binary.LittleEndian.Uint32(data[:4]))), 4, nil
	case "i64":
		if len(data) < 8 {
			return nil, 0, fmt.Errorf("insufficient data for i64")
		}
		return int64(binary.LittleEndian.Uint64(data[:8])), 8, nil
	case "f32":
		if len(data) < 4 {
			return nil, 0, fmt.Errorf("insufficient data for f32")
		}
		return float64(math.Float32frombits(binary.LittleEndian.Uint32(data[:4]))), 4, nil
	case "f64":
		if len(data) < 8 {
			return nil, 0, fmt.Errorf("insufficient data for f64")
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(data[:8])), 8, nil
	case "publicKey":
		if len(data) < 32 {
			return nil, 0, fmt.Errorf("insufficient data for publicKey")
		}
		var pk solanago.PublicKey
		copy(pk[:], data[:32])
		return pk.String(), 32, nil
	case "string":
		if len(data) < 4 {
			return nil, 0, fmt.Errorf("insufficient data for string length")
		}
		length := int(binary.LittleEndian.Uint32(data[:4]))
		if len(data) < 4+length {
			return nil, 0, fmt.Errorf("insufficient data for string")
		}
		return string(data[4 : 4+length]), 4 + length, nil
	case "bytes":
		if len(data) < 4 {
			return nil, 0, fmt.Errorf("insufficient data for bytes length")
		}
		length := int(binary.LittleEndian.Uint32(data[:4]))
		if len(data) < 4+length {
			return nil, 0, fmt.Errorf("insufficient data for bytes")
		}
		return hex.EncodeToString(data[4 : 4+length]), 4 + length, nil
	default:
		if types != nil {
			return decodeDefined(data, typ, types)
		}
		return nil, 0, fmt.Errorf("unsupported type: %q", typ)
	}
}

func decodeVec(data []byte, elemType interface{}, types map[string]idlTypeDef) (interface{}, int, error) {
	if len(data) < 4 {
		return nil, 0, fmt.Errorf("insufficient data for vec length")
	}
	count := int(binary.LittleEndian.Uint32(data[:4]))
	offset := 4
	result := make([]interface{}, 0, count)
	for i := 0; i < count; i++ {
		val, n, err := decodeField(data[offset:], elemType, types)
		if err != nil {
			return nil, 0, fmt.Errorf("vec[%d]: %w", i, err)
		}
		result = append(result, val)
		offset += n
	}
	return result, offset, nil
}

func decodeOption(data []byte, innerType interface{}, types map[string]idlTypeDef) (interface{}, int, error) {
	if len(data) < 1 {
		return nil, 0, fmt.Errorf("insufficient data for option")
	}
	if data[0] == 0 {
		return nil, 1, nil
	}
	val, n, err := decodeField(data[1:], innerType, types)
	if err != nil {
		return nil, 0, fmt.Errorf("option Some: %w", err)
	}
	return val, 1 + n, nil
}

func decodeDefined(data []byte, name string, types map[string]idlTypeDef) (interface{}, int, error) {
	if types == nil {
		return nil, 0, fmt.Errorf("no types for defined type %q", name)
	}
	def, ok := types[name]
	if !ok {
		return nil, 0, fmt.Errorf("unknown defined type %q", name)
	}
	result := make(map[string]interface{}, len(def.Type.Fields))
	offset := 0
	for _, f := range def.Type.Fields {
		val, n, err := decodeField(data[offset:], f.Type, types)
		if err != nil {
			return nil, 0, fmt.Errorf("defined %q field %q: %w", name, f.Name, err)
		}
		result[f.Name] = val
		offset += n
	}
	return result, offset, nil
}

func decodeArray(data []byte, spec interface{}, types map[string]idlTypeDef) (interface{}, int, error) {
	arr, ok := spec.([]interface{})
	if !ok || len(arr) != 2 {
		return nil, 0, fmt.Errorf("array spec must be [type, length]")
	}
	length, ok := arr[1].(float64)
	if !ok {
		return nil, 0, fmt.Errorf("array length must be a number")
	}
	count := int(length)
	offset := 0
	result := make([]interface{}, 0, count)
	for i := 0; i < count; i++ {
		val, n, err := decodeField(data[offset:], arr[0], types)
		if err != nil {
			return nil, 0, fmt.Errorf("array[%d]: %w", i, err)
		}
		result = append(result, val)
		offset += n
	}
	return result, offset, nil
}
