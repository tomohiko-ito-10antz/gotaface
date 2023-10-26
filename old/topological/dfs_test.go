package topological_test

import (
	"errors"
	"testing"

	"github.com/Jumpaku/gotaface/old/topological"
	"golang.org/x/exp/slices"
)

func TestDFS_OK(t *testing.T) {
	g := make([][]int, 8)
	g[0] = []int{5}
	g[1] = []int{3, 6}
	g[2] = []int{5, 7}
	g[3] = []int{0, 7}
	g[4] = []int{1, 2, 6}
	g[5] = []int{}
	g[6] = []int{7}
	g[7] = []int{0}

	type TestCase struct {
		root int
		want []int
	}
	testCases := []TestCase{
		{root: 0, want: []int{0, 5}},
		{root: 1, want: []int{1, 3, 0, 5, 7, 6}},
		{root: 2, want: []int{2, 5, 7, 0}},
		{root: 3, want: []int{3, 0, 5, 7}},
		{root: 4, want: []int{4, 1, 3, 0, 5, 7, 6, 2}},
		{root: 5, want: []int{5}},
		{root: 6, want: []int{6, 7, 0, 5}},
		{root: 7, want: []int{7, 0, 5}},
	}

	for _, testCase := range testCases {
		got := []int{}
		err := topological.DFS(g, testCase.root, func(v int) error {
			got = append(got, v)
			return nil
		})
		if err != nil {
			t.Errorf(`fail dfs`)
		}
		if !slices.Equal(got, testCase.want) {
			t.Errorf("not equal\n  got  = %#v\n  want = %#v", got, testCase.want)
		}
	}
}

func TestDFS_NG(t *testing.T) {
	g := make([][]int, 8)
	g[0] = []int{5}
	g[1] = []int{3, 6}
	g[2] = []int{5, 7}
	g[3] = []int{0, 7}
	g[4] = []int{1, 2, 6}
	g[5] = []int{}
	g[6] = []int{7}
	g[7] = []int{0}

	type TestCase struct {
		root  int
		isErr bool
	}
	testCases := []TestCase{
		{root: 0, isErr: true},
		{root: 1, isErr: true},
		{root: 2, isErr: true},
		{root: 3, isErr: true},
		{root: 4, isErr: true},
		{root: 5, isErr: false},
		{root: 6, isErr: true},
		{root: 7, isErr: true},
	}

	for _, testCase := range testCases {
		err := topological.DFS(g, testCase.root, func(v int) error {
			if v == 0 {
				return errors.New("error")
			}
			return nil
		})
		if (err != nil) != testCase.isErr {
			if err == nil {
				t.Errorf(`error not detected: %v`, testCase)
			} else {
				t.Errorf(`unexpected error detected: %v`, testCase)
			}
		}
	}
}
