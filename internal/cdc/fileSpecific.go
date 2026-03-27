package cdc

type FileSpecificChunker struct {
	normSizeBits int
	chunker      Chunker
	buf          []byte
}

var _ Chunker = (*FileSpecificChunker)(nil)

func (f *FileSpecificChunker) Next(data []byte) []int {
	if f.chunker != nil {
		return f.chunker.Next(data)
	}
	f.buf = append(f.buf, data...)
	if len(f.buf) < 8 {
		return nil
	}

	// Detect JPEG: FF D8 FF, then C0..CF or E0..EF or FE
	if f.buf[0] == 0xFF && f.buf[1] == 0xD8 && f.buf[2] == 0xFF &&
		((f.buf[3] >= 0xC0 && f.buf[3] <= 0xCF) || (f.buf[3] >= 0xE0 && f.buf[3] <= 0xEF) || f.buf[3] == 0xFE) {
		f.chunker = &jpegChunker{normSizeBits: f.normSizeBits, buf: f.buf[:4]}
		return f.chunker.Next(f.buf[4:])
	}
	// FIXME Detect AVIF and AVIS: bytes[4..7] == 'avif' or bytes[4..7] == 'avis' => custom chunking

	// No specific file type detected - fall back to regular CDC
	return f.cdc().Next(f.buf)
}

func (f *FileSpecificChunker) Flush() []int {
	if f.chunker != nil {
		return f.chunker.Flush()
	}
	return f.cdc().Flush()
}

func (f *FileSpecificChunker) cdc() Chunker {
	f.chunker = NewCDC(f.normSizeBits)
	return f.chunker
}
