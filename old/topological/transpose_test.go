package topological_test

import (
	"testing"

	"github.com/Jumpaku/gotaface/topological"
	"golang.org/x/exp/slices"
)

func TestTranspose(t *testing.T) {
	g := make([][]int, 8)
	g[0] = []int{5}
	g[1] = []int{3, 6}
	g[2] = []int{5, 7}
	g[3] = []int{0, 7}
	g[4] = []int{1, 2, 6}
	g[5] = []int{}
	g[6] = []int{7}
	g[7] = []int{0}

	got := topological.Transpose(g)
	want := make([][]int, 8)
	want[0] = []int{3, 7}
	want[1] = []int{4}
	want[2] = []int{4}
	want[3] = []int{1}
	want[4] = []int{}
	want[5] = []int{0, 2}
	want[6] = []int{1, 4}
	want[7] = []int{2, 3, 6}
	for i, want := range want {
		got := got[i]
		if !slices.Equal(got, want) {
			t.Errorf("i = %d\n  got  = %#v\n  want = %#v", i, got, want)
		}
	}
}
