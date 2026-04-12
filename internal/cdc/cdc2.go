package cdc

import "errors"

type cdc2Chunker struct {
	baseSize    int
	baseMask    int
	currentSize int
	currentMask int
	fingerprint int
}

var _ Chunker = (*cdc2Chunker)(nil)

func NewCDC2(targetSizeBits int) (Chunker, error) {
	if targetSizeBits < 6 || targetSizeBits > 30 {
		return nil, errors.New("targetSizeBits must be between 6 and 30 (inclusive)")
	}
	chunker := &cdc2Chunker{
		baseSize: 1 << (targetSizeBits - 1), // This produces an average chunk size
		baseMask: 1<<targetSizeBits - 1,     // slightly larger than 1 << targetSizeBits
	}
	chunker.Flush()
	return chunker, nil
}

func (c *cdc2Chunker) Flush() []int {
	defer func() {
		c.currentMask = c.baseMask
		c.currentSize = 0
		c.fingerprint = 0
	}()
	if c.currentSize == 0 {
		return nil
	}
	return []int{c.currentSize}
}

func (c *cdc2Chunker) Next(data []byte) []int { // NOSONAR: complexity is justified
	mask := c.currentMask
	startsAt := -c.currentSize
	fingerprint := c.fingerprint
	i := 0
	var chunkPositions []int

outer:
	for i < len(data) {
		// The table values have 31 bits. Due to the right shift, the current byte
		// and the previous 30 bytes influence the fingerprint. At the chunk start,
		// skip baseSize-30 bytes and use 30 bytes to warm up the fingerprint.
		// BaseSize can not be less than 32, and targetSizeBits must be at least 6.
		if startsAt+c.baseSize-30 > i {
			i = min(len(data), startsAt+c.baseSize-30)
		}
		if startsAt+c.baseSize > i {
			end := min(len(data), startsAt+c.baseSize)
			for ; i < end; i++ {
				fingerprint = (fingerprint >> 1) ^ table2[data[i]]
			}
		}

		// calculate next mask reduction position
		size := i - startsAt
		reduceAt := i + c.baseSize - size%c.baseSize

		// find end of chunk
		for ; i < len(data); i++ {
			if i == reduceAt {
				mask >>= 1
				reduceAt += c.baseSize
			}
			fingerprint = (fingerprint >> 1) + table2[data[i]]
			if (fingerprint & mask) == 0 {
				i++
				chunkPositions = append(chunkPositions, i-startsAt)
				mask = c.baseMask
				startsAt = i
				fingerprint = 0
				continue outer
			}
		}
	}
	c.currentSize = i - startsAt
	c.currentMask = mask
	return chunkPositions
}

// table contains the 31-bit integer data used for the rolling fingerprint function.
// FIXME generate new nicely distributed random values
var table2 = [256]int{
	0x032474b3, 0x75924e15, 0x29eb669, 0x7107a38c,
	0x6f0f57b9, 0x15ec79c7, 0x542ac0a, 0x263cec15,
	0x24a4451d, 0x59683605, 0x7b11ca1, 0x26ae15b2,
	0x35a89eac, 0x32f0038e, 0x65c0967, 0x2222fc4a,
	0x1a1e577a, 0x1fdbf750, 0x7dfcdcc, 0x630b4281,
	0x477c9056, 0x44612a62, 0x0943eb5, 0x0a8ec8d9,
	0x1fac8074, 0x60d318df, 0x50d8e3a, 0x2669dd25,
	0x74b10285, 0x4b9b6410, 0x7d55810, 0x6ff11b46,
	0x0dc3003e, 0x09b4009a, 0x529f22a, 0x21f77e56,
	0x002ee65d, 0x718ff813, 0x30db464, 0x380b72d1,
	0x1a6b31db, 0x1d69ea18, 0x2a7b8de, 0x054edd20,
	0x6b8b1dbf, 0x4f196a56, 0x47b6b17, 0x2894a6b7,
	0x7b0d6f91, 0x1de22bc8, 0x793e0fe, 0x4aed603c,
	0x7f63a45e, 0x2bb569d1, 0x5356b4b, 0x3f3e7a57,
	0x5e3596f1, 0x7cd23c1a, 0x0b3f71f, 0x7b93a3f9,
	0x27c9842f, 0x36e433ae, 0x58be306, 0x2a233fc6,
	0x0b7638e8, 0x3d2406d1, 0x4d0f133, 0x4ccb4be7,
	0x383594c8, 0x37af4429, 0x3799dbb, 0x0a494cbc,
	0x24cb0d1a, 0x102415a1, 0x493a9a4, 0x4b448631,
	0x346824b8, 0x0bc94f84, 0x6b55bb3, 0x79f6f297,
	0x007b3758, 0x7bca8b4d, 0x0b0c730, 0x3d623316,
	0x1242d5e8, 0x29858e30, 0x2a67386, 0x3fc420d4,
	0x2895f7e1, 0x5f6911ce, 0x5cfb7ce, 0x344c5c86,
	0x1839a861, 0x1ed3ed56, 0x3eda160, 0x2fe7f4e2,
	0x5cf6e151, 0x74af05c8, 0x367cc35, 0x0323b28d,
	0x5bb0ac2a, 0x3bee77ae, 0x33d0a9c, 0x631dbadb,
	0x226fb986, 0x3febabfa, 0x595bc2d, 0x0aa46b8b,
	0x139421ee, 0x1b1415da, 0x7e73631, 0x1fbe3356,
	0x206c622f, 0x5d272579, 0x0d8ec0d, 0x19580a3b,
	0x3a185e42, 0x6868732f, 0x29203a2, 0x5efe56d9,
	0x478ff9ed, 0x4a499ca7, 0x1682c70, 0x4e993eff,
	0x62e0b79a, 0x5712ffe8, 0x2bbd12a, 0x4315954f,
	0x138ab51a, 0x19907cee, 0x23523c8, 0x2e518332,
	0x323db7b2, 0x6f672a9d, 0x4903810, 0x3b5f1c4f,
	0x7ceca7d0, 0x24572374, 0x6e9513d, 0x3c1908c9,
	0x76a6339d, 0x7be9c679, 0x726f938, 0x5b27c959,
	0x0e4a8c9a, 0x0dfaa499, 0x6f4ac8b, 0x586259c6,
	0x6cb9617d, 0x5e2284d2, 0x3729865, 0x703e2633,
	0x0f0855a4, 0x2cf3a767, 0x7d6d14a, 0x343fe757,
	0x26352f85, 0x35b2bd76, 0x7d4a3fd, 0x436f3460,
	0x08cf4d44, 0x75f25899, 0x4e3b1e6, 0x561b3caa,
	0x6e9f0af7, 0x2c49a102, 0x3b332e7, 0x3f94ed1e,
	0x53046fa0, 0x6b9c2fb9, 0x3ebba8e, 0x2747bd58,
	0x06c8ec07, 0x028d0628, 0x1ca563d, 0x074d516d,
	0x3423ee99, 0x6f3120a8, 0x08860b8, 0x75118e81,
	0x7d1bd268, 0x75e5e88b, 0x4cd1dda, 0x5d7640f4,
	0x120833a2, 0x4ebcb044, 0x293f7bb, 0x4538f3e6,
	0x4486d0b4, 0x328851d3, 0x1a956b5, 0x137987e5,
	0x68ceac2c, 0x2f185cae, 0x35c4c1c, 0x40570bc7,
	0x0f64cbf6, 0x1705dcb9, 0x150e9f6, 0x6d74de02,
	0x705c7fa5, 0x725a6672, 0x3eb3bc6, 0x1cee565b,
	0x30c33e64, 0x3f21fe28, 0x643e514, 0x3eb01fa4,
	0x12306ed0, 0x38fc9686, 0x78507bb, 0x53fbe4d3,
	0x7fdcb6f6, 0x746ffde9, 0x5555266, 0x5ae1406e,
	0x2b5584b3, 0x15d45a60, 0x7d9bc83, 0x1264b23f,
	0x7e589ade, 0x54c7815c, 0x0d67d47, 0x7ea0a81c,
	0x10d25bfd, 0x4f9b6ab3, 0x587501d, 0x0dee38dc,
	0x0ce1c7d7, 0x7343d8f9, 0x703398d, 0x730281ed,
	0x750a8e59, 0x71adf736, 0x62b7def, 0x4937a02a,
	0x1fc8c4fc, 0x47a03f43, 0x1353ef9, 0x0413a867,
	0x3eda8be8, 0x719842c0, 0x6d40c1c, 0x2f9738b1,
	0x6511929a, 0x0bb226ea, 0x53a3c18, 0x11284c9c,
	0x23af373d, 0x1a866ffc, 0x0662799, 0x729e9830,
	0x18381b58, 0x231dc5ff, 0x78701f9, 0x471c65fe,
}
