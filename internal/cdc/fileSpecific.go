package cdc

import (
	"encoding/binary"
)

type fileSpecificChunker struct {
	cdcConfig *Config
	buf       []byte
	next      func() []int
	flush     func() []int
}

var _ Chunker = (*fileSpecificChunker)(nil)

func (f *fileSpecificChunker) Next(data []byte) []int {
	f.buf = append(f.buf, data...) // FIXME copy not needed if buf is empty
	return f.next()
}

func (f *fileSpecificChunker) Flush() []int {
	return f.flush()
}

func (c *Config) NewFileSpecificChunker() Chunker {
	chunker := &fileSpecificChunker{cdcConfig: c}
	chunker.reset()
	return chunker
}

func (f *fileSpecificChunker) reset() {
	f.buf = nil
	f.flush = f.flushBuffer
	f.next = f.collectHeader
}

// flushBuffer just returns the length of the buffered data if any as chunk
func (f *fileSpecificChunker) flushBuffer() []int {
	defer f.reset()
	if len(f.buf) == 0 {
		return nil
	}
	return []int{len(f.buf)}
}

// collectHeader collects data until we have at least 8 bytes buffered, then switches to detectFileType
func (f *fileSpecificChunker) collectHeader() []int {
	if len(f.buf) < 8 {
		return nil
	}
	f.next = f.detectFileType
	return f.next()
}

func (f *fileSpecificChunker) detectFileType() []int {
	if f.buf[0] == 0xFF && f.buf[1] == 0xD8 && f.buf[2] == 0xFF &&
		((f.buf[3] >= 0xC0 && f.buf[3] <= 0xCF) || (f.buf[3] >= 0xE0 && f.buf[3] <= 0xEF) || f.buf[3] == 0xFE) {
		// JPEG detected: FF D8 FF, then C0..CF or E0..EF or FE
		f.switchToJpeg()
	} else if string(f.buf[4:8]) == "ftyp" {
		// ISO-BMFF (avif, heic and similar detected): magic string in bytes 4..7
		f.switchToISO()
	} else {
		// No specific file type detected - fall back to regular CDC
		f.switchToCDC()
	}
	return f.next()
}

func (f *fileSpecificChunker) switchToCDC() {
	cdc := f.cdcConfig.NewCDC()
	f.next = func() []int {
		defer func() { f.buf = nil }()
		return cdc.Next(f.buf)
	}
	f.flush = func() []int {
		defer f.reset()
		return append(cdc.Next(f.buf), cdc.Flush()...)
	}
}

func (f *fileSpecificChunker) switchToJpeg() {
	lookForSosAt := 4
	f.next = func() []int {
		for i := lookForSosAt; i < len(f.buf)-1; i++ {
			if i == 256*1024 {
				f.switchToCDC()
				return f.next()
			}
			if f.buf[i] == 0xFF && f.buf[i+1] == 0xDA {
				// JPEG Start of Scan (SOS) marker found
				f.buf = f.buf[i+2:]
				f.switchToCDC()
				return append([]int{i + 2}, f.next()...)
			}
		}
		lookForSosAt = len(f.buf) - 1
		return nil
	}
}

func (f *fileSpecificChunker) switchToISO() {
	// For simplicity, ignore extended size boxes and assume the box size fits into int
	nextBox := int(binary.BigEndian.Uint32(f.buf[0:4]))
	f.next = func() []int {
		for len(f.buf) >= nextBox+8 {
			if nextBox >= 256*1024 { // This is crap. We need to switch to cdc earlier if the nextBox is large.
				f.switchToCDC()
				return f.next()
			}
			if string(f.buf[nextBox+4:nextBox+8]) == "mdat" {
				// First media data box found
				f.buf = f.buf[nextBox+8:]
				f.switchToCDC()
				return append([]int{nextBox + 8}, f.next()...)
			}
			nextBox = nextBox + int(binary.BigEndian.Uint32(f.buf[nextBox:nextBox+4]))
		}
		return nil
	}
}
