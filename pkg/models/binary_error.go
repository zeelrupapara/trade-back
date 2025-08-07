package models

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

// EncodeError creates a binary error message
func EncodeError(errorMsg string) ([]byte, error) {
	errorBytes := []byte(errorMsg)
	errorLen := len(errorBytes)
	
	// Header(8) + ErrorLen(2) + ErrorMsg + Checksum(4)
	totalSize := 8 + 2 + errorLen + 4
	buf := make([]byte, totalSize)
	
	// Write header
	binary.LittleEndian.PutUint16(buf[0:2], MsgTypeError)
	binary.LittleEndian.PutUint16(buf[2:4], 0) // No symbol for general errors
	binary.LittleEndian.PutUint32(buf[4:8], uint32(time.Now().Unix()))
	
	// Write error message
	offset := 8
	binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(errorLen))
	copy(buf[offset+2:offset+2+errorLen], errorBytes)
	
	// Calculate and write checksum
	payloadSize := totalSize - 4
	checksum := crc32.ChecksumIEEE(buf[:payloadSize])
	binary.LittleEndian.PutUint32(buf[payloadSize:], checksum)
	
	return buf, nil
}