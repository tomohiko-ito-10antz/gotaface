package topological_test

import (
	"testing"

	"github.com/Jumpaku/gotaface/old/test/assert"
	"github.com/Jumpaku/gotaface/old/topological"
	"golang.org/x/exp/slices"
)

func TestTopologicalSort_OK(t *testing.T) {
	g := make([][]int, 8)
	g[0] = []int{5}
	g[1] = []int{3, 6}
	g[2] = []int{5, 7}
	g[3] = []int{0, 7}
	g[4] = []int{1, 2, 6}
	g[5] = []int{}
	g[6] = []int{7}
	g[7] = []int{0}

	got, ok := topological.Sort(g)
	want := []int{4, 1, 1, 2, 0, 5, 2, 3}
	assert.Equal(t, ok, true)
	if !slices.Equal(got, want) {
		t.Errorf("got != want\n  got  = %#v\n  want = %#v", got, want)
	}
}

func TestTopologicalSort_NG(t *testing.T) {
	g := make([][]int, 2)
	g[0] = []int{1}
	g[1] = []int{1}

	got, ok := topological.Sort(g)
	want := []int{}
	assert.Equal(t, ok, false)
	if !slices.Equal(got, []int{}) {
		t.Errorf("got != want\n  got  = %#v\n  want = %#v", got, want)
	}
}
