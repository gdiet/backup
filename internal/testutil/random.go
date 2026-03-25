package testutil

// LCG is a linear congruential deterministic pseudo random number generator,
// see https://en.wikipedia.org/wiki/Linear_congruential_generator. Do not change
// parameters or implementation, since the output is assumed to be reproducible
// in tests.
type LCG struct {
	// Seed is the current state of the generator. Don't use the lowest bits, they are not very random.
	Seed uint32
}

func (l *LCG) Next() uint32 {
	l.Seed = 1_664_525*l.Seed + 1_013_904_223
	return l.Seed
}

func PseudoRandomData(seed uint32, length int) []byte {
	b := make([]byte, length)
	l := LCG{Seed: seed}
	for i := 0; i < length; i++ {
		b[i] = byte(l.Next() >> 16)
	}
	return b
}

func PseudoRandomText(seed uint32, length int) string {
	charset := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789,.-     "
	b := make([]byte, length)
	l := LCG{Seed: seed}
	for i := 0; i < length; i++ {
		b[i] = charset[(l.Next()>>16)%uint32(len(charset))]
	}
	return string(b)
}
