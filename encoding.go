package crdt

import (
	"bytes"
	"encoding/gob"
)

// gobEncode encodes multiple values into a single byte slice using gob.
// This reduces error-checking boilerplate across all CRDT marshal methods.
func gobEncode(values ...any) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	for _, v := range values {
		if err := enc.Encode(v); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// gobDecode decodes values from data using gob. Each target must be a pointer.
func gobDecode(data []byte, targets ...any) error {
	dec := gob.NewDecoder(bytes.NewReader(data))
	for _, t := range targets {
		if err := dec.Decode(t); err != nil {
			return err
		}
	}
	return nil
}
