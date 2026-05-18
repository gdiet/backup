package cdc

type jpegChunker struct {
	cdcConfig *Config
	buf       []byte
	next      func() []int
	flush     func() []int
}

var _ Chunker = (*jpegChunker)(nil)

func (f *jpegChunker) Next(data []byte) []int {
	f.buf = append(f.buf, data...) // FIXME copy not needed if buf is empty
	return f.next()
}

func (f *jpegChunker) Flush() []int {
	return f.flush()
}

// NewJpegChunker provides a chunker that detects certain file types based on the first few
// bytes and then applies specific chunking strategies, e.g. chunking after the JPEG "SOS" marker,
// to improve deduplication of JPEG files differing only in metadata. It uses the regular CDC for
// unknown types.
//
// This chunker is EXPERIMENTAL. It might not work as expected, and the API is subject to change.
func (c *Config) NewJpegChunker() Chunker {
	chunker := &jpegChunker{cdcConfig: c}
	chunker.reset()
	return chunker
}

func (f *jpegChunker) reset() {
	f.buf = nil
	f.flush = f.flushBuffer
	f.next = f.collectHeader
}

// flushBuffer just returns the length of the buffered data if any as chunk
func (f *jpegChunker) flushBuffer() []int {
	defer f.reset()
	if len(f.buf) == 0 {
		return nil
	}
	return []int{len(f.buf)}
}

// collectHeader collects data until we have at least 8 bytes buffered, then switches to detectFileType
func (f *jpegChunker) collectHeader() []int {
	if len(f.buf) < 8 {
		return nil
	}
	f.next = f.detectFileType
	return f.next()
}

func (f *jpegChunker) detectFileType() []int {
	if f.buf[0] == 0xFF && f.buf[1] == 0xD8 && f.buf[2] == 0xFF &&
		((f.buf[3] >= 0xC0 && f.buf[3] <= 0xCF) || (f.buf[3] >= 0xE0 && f.buf[3] <= 0xEF) || f.buf[3] == 0xFE) {
		// JPEG detected: FF D8 FF, then C0...CF or E0...EF or FE
		f.switchToJpeg()
	} else {
		// No specific file type detected - fall back to regular CDC
		f.switchToCDC()
	}
	return f.next()
}

func (f *jpegChunker) switchToCDC() {
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

func (f *jpegChunker) switchToJpeg() {
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
