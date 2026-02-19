package utils

import "encoding/json"

// MinifyABI removes whitespace and newlines from ABI JSON to reduce storage size.
func MinifyABI(abi []byte) ([]byte, error) {
	var v interface{}
	if err := json.Unmarshal(abi, &v); err != nil {
		return nil, err
	}
	return json.Marshal(v)
}
