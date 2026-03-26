package cdc

// JpegChunker chunks after the Start of Scan (SOS) marker of a JPEG file, then using regular CDC.
// Use only if JPEG is detected in the first 4 bytes of the file: FF D8 FF, then C0..CF or E0..EF or FE.
// If the SOS marker is not found in the first 256 kB, the chunker falls back to regular CDC chunking.
type JpegChunker struct {
	normSizeBits int
	chunker      Chunker
	buf          []byte
	endsWithFF   bool
}

var _ Chunker = (*JpegChunker)(nil)

func (f *JpegChunker) Next(data []byte) []int {
	if f.chunker != nil {
		return f.chunker.Next(data)
	}
	if len(data) == 0 {
		return nil
	}
	// Find FF DA (SOS marker) in data
	if f.endsWithFF {
		// Check for DA after FF
		if data[0] == 0xDA {
			// SOS found: buf already contains FF, +1 accounts for DA
			return append([]int{len(f.buf) + 1}, f.cdc().Next(data[1:])...)
		}
		f.endsWithFF = false
	}
	// Check for FF
	for i, b := range data {
		if i+len(f.buf) >= 256*1024 {
			break
		}
		if b == 0xFF {
			f.buf = append(f.buf, data[:i+1]...)
			f.endsWithFF = true
			// Recurse to check for DA
			return f.Next(data[i+1:])
		}
	}
	f.buf = append(f.buf, data...)
	if len(f.buf) >= 256*1024 {
		return f.cdc().Next(f.buf)
	}
	return nil
}

func (f *JpegChunker) Flush() []int {
	if f.chunker != nil {
		return f.chunker.Flush()
	}
	return f.cdc().Flush()
}

func (f *JpegChunker) cdc() Chunker {
	f.chunker = NewCDC(f.normSizeBits)
	return f.chunker
}
