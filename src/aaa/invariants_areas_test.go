package aaa

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateAreasInvariants(t *testing.T) {
	t.Run("empty area should cause panic", func(t *testing.T) {
		require.Panics(t, func() {
			validateAreasInvariants(areas{
				area{off: 0, len: 0},
			})
		})
	})

	t.Run("areas not ordered by position should cause panic", func(t *testing.T) {
		require.Panics(t, func() {
			validateAreasInvariants(areas{
				area{off: 18, len: 4},
				area{off: 10, len: 4},
			})
		})
	})

	t.Run("overlapping areas should cause panic", func(t *testing.T) {
		require.Panics(t, func() {
			validateAreasInvariants(areas{
				area{off: 8, len: 4},
				area{off: 10, len: 4},
			})
		})
	})

	t.Run("well-formed areas should not cause panic", func(t *testing.T) {
		require.NotPanics(t, func() {
			validateAreasInvariants(areas{
				area{off: 8, len: 4},
				area{off: 18, len: 4},
				area{off: 23, len: 4},
			})
		})
	})
}
