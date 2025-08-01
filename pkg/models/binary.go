package models

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"time"
)

// Message types for binary protocol
const (
	MsgTypePriceUpdate   = 0x0001
	MsgTypeEnigmaUpdate  = 0x0002
	MsgTypeSessionChange = 0x0003
	MsgTypeMarketWatch   = 0x0004
	MsgTypeHeartbeat     = 0x0005
	MsgTypeError         = 0x00FF
)

// BinaryMessage represents the header of all binary messages
type BinaryMessage struct {
	Type      uint16
	SymbolLen uint16
	Timestamp uint32
	Symbol    string
	Checksum  uint32
}

// BinaryPriceData represents price data in binary format
type BinaryPriceData struct {
	BinaryMessage
	Price  float64
	Bid    float64
	Ask    float64
	Volume float64
}

// EncodePriceData converts PriceData to binary format
func EncodePriceData(price *PriceData) ([]byte, error) {
	symbolBytes := []byte(price.Symbol)
	symbolLen := len(symbolBytes)
	
	// Calculate total message size: header(8) + symbol + pricedata(32) + checksum(4)
	totalSize := 8 + symbolLen + 32 + 4
	buf := make([]byte, totalSize)
	
	// Write header
	binary.LittleEndian.PutUint16(buf[0:2], MsgTypePriceUpdate)
	binary.LittleEndian.PutUint16(buf[2:4], uint16(symbolLen))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(price.Timestamp.Unix()))
	
	// Write symbol
	copy(buf[8:8+symbolLen], symbolBytes)
	
	// Write price data (32 bytes total)
	offset := 8 + symbolLen
	binary.LittleEndian.PutUint64(buf[offset:offset+8], math.Float64bits(price.Price))
	binary.LittleEndian.PutUint64(buf[offset+8:offset+16], math.Float64bits(price.Bid))
	binary.LittleEndian.PutUint64(buf[offset+16:offset+24], math.Float64bits(price.Ask))
	binary.LittleEndian.PutUint64(buf[offset+24:offset+32], math.Float64bits(price.Volume))
	
	// Calculate and write checksum (exclude the checksum bytes themselves)
	payloadSize := totalSize - 4
	checksum := crc32.ChecksumIEEE(buf[:payloadSize])
	binary.LittleEndian.PutUint32(buf[payloadSize:], checksum)
	
	return buf, nil
}

// DecodePriceData converts binary data back to PriceData
func DecodePriceData(data []byte) (*PriceData, error) {
	if len(data) < 44 { // Minimum size: 8 + 1 + 32 + 4
		return nil, fmt.Errorf("insufficient data length: %d", len(data))
	}
	
	// Read header
	msgType := binary.LittleEndian.Uint16(data[0:2])
	if msgType != MsgTypePriceUpdate {
		return nil, fmt.Errorf("invalid message type: %d", msgType)
	}
	
	symbolLen := binary.LittleEndian.Uint16(data[2:4])
	timestamp := binary.LittleEndian.Uint32(data[4:8])
	
	// Validate total length
	expectedLen := 8 + int(symbolLen) + 32 + 4
	if len(data) != expectedLen {
		return nil, fmt.Errorf("invalid data length: expected %d, got %d", expectedLen, len(data))
	}
	
	// Verify checksum
	payloadSize := len(data) - 4
	expectedChecksum := binary.LittleEndian.Uint32(data[payloadSize:])
	actualChecksum := crc32.ChecksumIEEE(data[:payloadSize])
	if expectedChecksum != actualChecksum {
		return nil, fmt.Errorf("checksum mismatch: expected %d, got %d", expectedChecksum, actualChecksum)
	}
	
	// Read symbol
	symbol := string(data[8 : 8+symbolLen])
	
	// Read price data
	offset := 8 + int(symbolLen)
	price := math.Float64frombits(binary.LittleEndian.Uint64(data[offset : offset+8]))
	bid := math.Float64frombits(binary.LittleEndian.Uint64(data[offset+8 : offset+16]))
	ask := math.Float64frombits(binary.LittleEndian.Uint64(data[offset+16 : offset+24]))
	volume := math.Float64frombits(binary.LittleEndian.Uint64(data[offset+24 : offset+32]))
	
	return &PriceData{
		Symbol:    symbol,
		Price:     price,
		Bid:       bid,
		Ask:       ask,
		Volume:    volume,
		Timestamp: time.Unix(int64(timestamp), 0),
	}, nil
}

// EncodeEnigmaData converts EnigmaData to binary format
func EncodeEnigmaData(enigma *EnigmaData) ([]byte, error) {
	symbolBytes := []byte(enigma.Symbol)
	symbolLen := len(symbolBytes)
	
	// Header(8) + Symbol + EnigmaData(88) + Checksum(4)
	// EnigmaData: Level(8) + ATH(8) + ATL(8) + 8 fib levels(8x8=64)
	totalSize := 8 + symbolLen + 88 + 4
	buf := make([]byte, totalSize)
	
	// Write header
	binary.LittleEndian.PutUint16(buf[0:2], MsgTypeEnigmaUpdate)
	binary.LittleEndian.PutUint16(buf[2:4], uint16(symbolLen))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(time.Now().Unix()))
	
	// Write symbol
	copy(buf[8:8+symbolLen], symbolBytes)
	
	// Write enigma data
	offset := 8 + symbolLen
	binary.LittleEndian.PutUint64(buf[offset:offset+8], math.Float64bits(enigma.Level))
	binary.LittleEndian.PutUint64(buf[offset+8:offset+16], math.Float64bits(enigma.ATH))
	binary.LittleEndian.PutUint64(buf[offset+16:offset+24], math.Float64bits(enigma.ATL))
	
	// Write fibonacci levels (8 levels x 8 bytes = 64 bytes)
	fibOffset := offset + 24
	fibLevels := []float64{
		enigma.FibLevels.L0,
		enigma.FibLevels.L236,
		enigma.FibLevels.L382,
		enigma.FibLevels.L50,
		enigma.FibLevels.L618,
		enigma.FibLevels.L786,
		enigma.FibLevels.L100,
		0, // Reserved for future level
	}
	
	for i, level := range fibLevels {
		binary.LittleEndian.PutUint64(buf[fibOffset+i*8:fibOffset+(i+1)*8], math.Float64bits(level))
	}
	
	// Calculate and write checksum
	payloadSize := totalSize - 4
	checksum := crc32.ChecksumIEEE(buf[:payloadSize])
	binary.LittleEndian.PutUint32(buf[payloadSize:], checksum)
	
	return buf, nil
}

// EncodeHeartbeat creates a minimal heartbeat message
func EncodeHeartbeat() ([]byte, error) {
	// Minimal message: just header + checksum
	buf := make([]byte, 12) // 8 bytes header + 4 bytes checksum
	
	binary.LittleEndian.PutUint16(buf[0:2], MsgTypeHeartbeat)
	binary.LittleEndian.PutUint16(buf[2:4], 0) // No symbol
	binary.LittleEndian.PutUint32(buf[4:8], uint32(time.Now().Unix()))
	
	// Calculate checksum
	checksum := crc32.ChecksumIEEE(buf[:8])
	binary.LittleEndian.PutUint32(buf[8:], checksum)
	
	return buf, nil
}

// BatchEncodePrices efficiently encodes multiple prices in one message
func BatchEncodePrices(prices []*PriceData) ([]byte, error) {
	if len(prices) == 0 {
		return nil, fmt.Errorf("no prices to encode")
	}
	
	// Calculate total size needed
	totalSize := 8 // Header
	for _, price := range prices {
		totalSize += 2 + len(price.Symbol) + 32 // SymbolLen(2) + Symbol + PriceData(32)
	}
	totalSize += 4 // Checksum
	
	buf := make([]byte, totalSize)
	
	// Write header with batch type
	binary.LittleEndian.PutUint16(buf[0:2], MsgTypePriceUpdate|0x8000) // Set batch bit
	binary.LittleEndian.PutUint16(buf[2:4], uint16(len(prices)))       // Number of prices
	binary.LittleEndian.PutUint32(buf[4:8], uint32(time.Now().Unix()))
	
	offset := 8
	for _, price := range prices {
		symbolBytes := []byte(price.Symbol)
		symbolLen := len(symbolBytes)
		
		// Write symbol length and symbol
		binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(symbolLen))
		copy(buf[offset+2:offset+2+symbolLen], symbolBytes)
		offset += 2 + symbolLen
		
		// Write price data
		binary.LittleEndian.PutUint64(buf[offset:offset+8], math.Float64bits(price.Price))
		binary.LittleEndian.PutUint64(buf[offset+8:offset+16], math.Float64bits(price.Bid))
		binary.LittleEndian.PutUint64(buf[offset+16:offset+24], math.Float64bits(price.Ask))
		binary.LittleEndian.PutUint64(buf[offset+24:offset+32], math.Float64bits(price.Volume))
		offset += 32
	}
	
	// Calculate and write checksum
	payloadSize := totalSize - 4
	checksum := crc32.ChecksumIEEE(buf[:payloadSize])
	binary.LittleEndian.PutUint32(buf[payloadSize:], checksum)
	
	return buf, nil
}

// GetMessageType returns the message type from binary data
func GetMessageType(data []byte) (uint16, error) {
	if len(data) < 2 {
		return 0, fmt.Errorf("insufficient data for message type")
	}
	return binary.LittleEndian.Uint16(data[0:2]), nil
}

// ValidateChecksum verifies the integrity of binary message
func ValidateChecksum(data []byte) error {
	if len(data) < 4 {
		return fmt.Errorf("insufficient data for checksum")
	}
	
	payloadSize := len(data) - 4
	expectedChecksum := binary.LittleEndian.Uint32(data[payloadSize:])
	actualChecksum := crc32.ChecksumIEEE(data[:payloadSize])
	
	if expectedChecksum != actualChecksum {
		return fmt.Errorf("checksum mismatch: expected %d, got %d", expectedChecksum, actualChecksum)
	}
	
	return nil
}