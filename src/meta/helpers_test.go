package meta

import "testing"

func TestEqualsInt(t *testing.T) {
	tests := []struct {
		a, b []int
		want bool
		name string
	}{
		{[]int{1, 2, 3}, []int{1, 2, 3}, true, "equal"},
		{[]int{1, 2, 3}, []int{1, 2, 4}, false, "different values"},
		{[]int{1, 2}, []int{1, 2, 3}, false, "different length"},
		{[]int{}, []int{}, true, "both empty"},
		{nil, nil, true, "both nil"},
		{nil, []int{}, true, "nil vs empty"},
		{[]int{}, nil, true, "empty vs nil"},
		{[]int{1}, nil, false, "one nil, one not"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Equals(tc.a, tc.b); got != tc.want {
				t.Errorf("Equals(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestEqualsString(t *testing.T) {
	tests := []struct {
		a, b []string
		want bool
		name string
	}{
		{[]string{"a", "b"}, []string{"a", "b"}, true, "equal"},
		{[]string{"a", "b"}, []string{"a", "c"}, false, "different values"},
		{[]string{"a"}, []string{"a", "b"}, false, "different length"},
		{[]string{}, []string{}, true, "both empty"},
		{nil, nil, true, "both nil"},
		{nil, []string{}, true, "nil vs empty"},
		{[]string{}, nil, true, "empty vs nil"},
		{[]string{"a"}, nil, false, "one nil, one not"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Equals(tc.a, tc.b); got != tc.want {
				t.Errorf("Equals(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}
