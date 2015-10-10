package stemma

import (
	"encoding/binary"
	"fmt"
	"io"
)

func marshalBytes(w io.Writer, buf []byte) error {
	bufLen := uint16(len(buf))
	if err := binary.Write(w, binary.LittleEndian, bufLen); err != nil {
		return fmt.Errorf("unable to encode buffer length: %s", err)
	}

	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("unable to write from buffer: %s", err)
	}

	return nil
}

func unmarshalBytes(r io.Reader) (buf []byte, err error) {
	var bufLen uint16
	if err := binary.Read(r, binary.LittleEndian, &bufLen); err != nil {
		return nil, fmt.Errorf("unable to decode length of buffer: %s", err)
	}

	buf = make([]byte, bufLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("unable to read into buffer: %s", err)
	}

	return buf, nil
}
