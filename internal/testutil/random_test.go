package testutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPseudoRandomData(t *testing.T) {
	data := PseudoRandomData(42, 10)
	expected := []byte{0x99, 0x8f, 0xc8, 0xf9, 0x2b, 0x91, 0x81, 0x53, 0xb2, 0xa0}
	require.Equal(t, expected, data)
}

func TestPseudoRandomText(t *testing.T) {
	text := PseudoRandomText(42, 42)
	expected := "rJGzXb1.go34eKE sIP0CaaF hBJk6bbcDIHv a3jf"
	require.Equal(t, expected, text)
}
