package cdc

import "errors"

type Chunker interface {
	// Next returns the lengths of all complete chunks in the data. Chunks may span multiple calls.
	Next(data []byte) []int
	// Flush returns the length of the last incomplete chunk if any and resets the chunker.
	Flush() []int
}

type CdcConfig struct {
	targetSizeBits int
}

func NewCdcConfig(targetSizeBits int) (*CdcConfig, error) {
	if targetSizeBits < 6 || targetSizeBits > 30 {
		return nil, errors.New("targetSizeBits must be between 6 and 30 (inclusive)")
	}
	return &CdcConfig{targetSizeBits}, nil
}

type cdcChunker struct {
	baseSize    int
	baseMask    int
	chunkStart  int
	currentMask int
	fingerprint int
}

var _ Chunker = (*cdcChunker)(nil)

func (c *CdcConfig) NewCDC() Chunker {
	chunker := &cdcChunker{
		baseSize: 1 << (c.targetSizeBits - 1), // Produces an average chunk size of
		baseMask: (1 << c.targetSizeBits) - 1, // approximately 1 << targetSizeBits.
	}
	chunker.Flush()
	return chunker
}

func (c *cdcChunker) Flush() []int {
	defer func() {
		c.currentMask = c.baseMask
		c.chunkStart = 0
		c.fingerprint = 0
	}()
	if c.chunkStart == 0 {
		return nil
	}
	return []int{-c.chunkStart}
}

func (c *cdcChunker) Next(data []byte) []int { // NOSONAR: complexity is justified
	// Copying the struct fields to local vars does not help.
	// It reduced performance by 5-10 % (Go 1.25.5, Intel i7-1355U).
	i := 0
	var chunkPositions []int

outer:
	for i < len(data) {
		// The table values have 31 bits. Due to the right shift, the current byte
		// and the previous 30 bytes influence the fingerprint. At the chunk start,
		// skip baseSize-30 bytes and use 30 bytes to warm up the fingerprint.
		// BaseSize can not be less than 32, and targetSizeBits must be at least 6.
		if c.chunkStart+c.baseSize-30 > i {
			i = min(len(data), c.chunkStart+c.baseSize-30)
		}
		if c.chunkStart+c.baseSize > i {
			end := min(len(data), c.chunkStart+c.baseSize)
			for ; i < end; i++ {
				c.fingerprint = (c.fingerprint >> 1) ^ table[data[i]]
			}
		}

		// calculate next mask reduction position
		size := i - c.chunkStart
		reduceAt := i + c.baseSize - size%c.baseSize

		// find end of chunk
		for ; i < len(data); i++ {
			if i == reduceAt { //       We could take this check out of the hot loop,
				c.currentMask >>= 1 //  but on i7-1355U this does not speed up things.
				reduceAt += c.baseSize
			}
			c.fingerprint = (c.fingerprint >> 1) + table[data[i]]
			if (c.fingerprint & c.currentMask) == 0 {
				i++
				chunkPositions = append(chunkPositions, i-c.chunkStart)
				c.currentMask = c.baseMask
				c.chunkStart = i
				c.fingerprint = 0
				continue outer
			}
		}
	}
	c.chunkStart = c.chunkStart - i
	return chunkPositions
}

// table contains the 31-bit integer data used for the rolling fingerprint function.
// The table values have each bit set in exactly 128 entries, ensuring a good
// distribution of bits for the fingerprint calculation.
var table = [256]int{
	0x22612e91, 0x1170f0e6, 0x3303b39b, 0x66bd6edd,
	0x01d2f2af, 0x231317fa, 0x2a289c7e, 0x36bd43c9,
	0x1bb014c6, 0x39b82bbf, 0x32ad8dfe, 0x54338a27,
	0x5dfd4610, 0x641b1ed2, 0x464b3e2d, 0x30642c32,
	0x08a2c072, 0x471b5497, 0x3d7a654e, 0x58c1a3e4,
	0x23a484f4, 0x49843509, 0x218c2950, 0x52572f6e,
	0x1c433479, 0x25f4c5ae, 0x5b6d64bc, 0x3f1f9806,
	0x5785d1bd, 0x4e27cfa3, 0x007d56be, 0x0c12e1dd,
	0x364e7dc3, 0x6bd7d89a, 0x1cbb7170, 0x6063a130,
	0x4db1ec82, 0x4b9232ec, 0x68652cec, 0x385d7f65,
	0x2379eca0, 0x4131907e, 0x106cb914, 0x4dee34be,
	0x6afabe59, 0x1edaf97f, 0x526a7e6b, 0x492f5102,
	0x4fe73be2, 0x560d24d9, 0x025dfed8, 0x1305d8a6,
	0x2abe4b97, 0x4062b20d, 0x5a10fa46, 0x26d11bf6,
	0x61aadec3, 0x7db6b0b4, 0x19e9dd99, 0x5f94f409,
	0x6540f9c9, 0x4acfe0f3, 0x46df254c, 0x3671050d,
	0x3702ddb9, 0x072c59e3, 0x34306b60, 0x172b6008,
	0x59811a7b, 0x30522671, 0x51e219ef, 0x5f3f0a79,
	0x27e99be2, 0x60846cc1, 0x464ed3ad, 0x7a090f46,
	0x3c31e5b2, 0x76a7ffb0, 0x36b1c337, 0x0e09462c,
	0x6e70db9a, 0x28257e71, 0x3ec84a59, 0x529ac661,
	0x610ff9fa, 0x7aa023b3, 0x1decaf62, 0x5b5fac14,
	0x38d7582b, 0x12e24616, 0x5dc4cd4c, 0x5c55c4f3,
	0x4d0c7992, 0x780fe3ad, 0x3e12c00e, 0x71892389,
	0x25c08386, 0x0b7fe407, 0x72c1d05c, 0x72ceac7c,
	0x6a8d9f79, 0x30c1ff57, 0x2dc2c4a9, 0x4fa2d586,
	0x02fbdb64, 0x50b45931, 0x0125dbe5, 0x65753475,
	0x056cd423, 0x1a9f5465, 0x2da3cd41, 0x3369f68c,
	0x5f680814, 0x0285c614, 0x5ac9946f, 0x6b4b8f0c,
	0x23183b4b, 0x2f64362a, 0x1d1b7e13, 0x56199e01,
	0x6c0f77c5, 0x61205bca, 0x39ffc2a4, 0x0813d309,
	0x099f028d, 0x6ab61b57, 0x5c2fe165, 0x18567e97,
	0x1b6b32d3, 0x7cd73ba0, 0x7a2ea353, 0x70bb42be,
	0x27e2250f, 0x34e1a18e, 0x08c5ba4a, 0x5e6c294f,
	0x4e5e2407, 0x1c3e9cf1, 0x63e1b51a, 0x563076a0,
	0x3ae575e5, 0x6d601e3b, 0x6ccdcd71, 0x57a62cf4,
	0x632e0239, 0x488ca595, 0x2514394c, 0x2781d88c,
	0x53846075, 0x0442819d, 0x0b38a3c1, 0x28ce59f6,
	0x52508ef8, 0x690bcfb8, 0x6d7e9d2d, 0x766de29c,
	0x2c4d75d0, 0x68790e5e, 0x16f97228, 0x0110cad4,
	0x72462c2b, 0x2ccd1a6e, 0x6728dad1, 0x2db6b1f4,
	0x4520f6fb, 0x69a59884, 0x369e13d4, 0x2222b521,
	0x42773fe1, 0x3891b606, 0x058ef3d2, 0x2c720b5a,
	0x3ba24426, 0x6f357e67, 0x285fb907, 0x5af5a59c,
	0x569e3edb, 0x0b046707, 0x44d20887, 0x034aceea,
	0x53c71d0e, 0x2faa5f89, 0x06f18ffc, 0x1ddc49b0,
	0x4f406f39, 0x5f514f11, 0x331a5fd0, 0x577f50d8,
	0x4adc8563, 0x35c76d97, 0x0d104f09, 0x6b71648c,
	0x37b9c4df, 0x6c457b3e, 0x0c97591e, 0x77cd2174,
	0x30340a37, 0x476d8c4e, 0x555a1980, 0x1456f048,
	0x586a8a66, 0x277b684a, 0x3816066d, 0x378277ce,
	0x658682a9, 0x57cb4296, 0x35ada011, 0x297f6b47,
	0x008423d2, 0x58bcdd39, 0x67f639ce, 0x4514afc2,
	0x391393be, 0x20b9723a, 0x7efc1093, 0x4bb28ed2,
	0x1e5c4bb9, 0x61778960, 0x1d9685c0, 0x143c8c22,
	0x78fba75b, 0x1589a1e9, 0x516a40a7, 0x44db84f3,
	0x7ae18838, 0x4d8b8e64, 0x6c9b609d, 0x2bf0bd4b,
	0x5522cb79, 0x32652269, 0x0fdb180b, 0x4edbedbb,
	0x419e1980, 0x0712f4e0, 0x79aabeb5, 0x481f560d,
	0x695e289c, 0x65b0f2b3, 0x75785116, 0x1f85469e,
	0x1a9cffab, 0x1ce2752a, 0x1beee272, 0x36b62112,
	0x11a3124e, 0x3756b0dc, 0x2d58bbac, 0x51c6b6bf,
	0x34108377, 0x72fba16f, 0x41c9d065, 0x34590797,
	0x38b4057b, 0x623e955b, 0x5aacc345, 0x0f8f7b09,
}
