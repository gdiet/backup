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

func (f JpegChunker) Next(data []byte) []int {
	if f.chunker != nil {
		return f.chunker.Next(data)
	}
	if len(data) == 0 {
		return nil
	}
	// Check for DA after FF
	if f.endsWithFF && data[0] == 0xDA {
		return append([]int{len(f.buf) + 1}, f.cdc().Next(data[1:])...)
	}
	// Find FF in data
	for i, b := range data {
		if i+len(f.buf) >= 256*1024 {
			break
		}
		if b == 0xff {
			f.buf = append(f.buf, data[:i]...)
			f.endsWithFF = true
			return f.Next(data[i:])
		}
	}
	f.buf = append(f.buf, data...)
	if len(f.buf) >= 256*1024 {
		return f.cdc().Next(f.buf)
	}
	return nil
}

func (f JpegChunker) Flush() []int {
	if f.chunker != nil {
		return f.chunker.Flush()
	}
	return f.cdc().Flush()
}

func (f JpegChunker) cdc() Chunker {
	f.chunker = NewCDC(f.normSizeBits)
	return f.chunker
}
