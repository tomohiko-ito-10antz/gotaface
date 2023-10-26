package topological

import "golang.org/x/exp/slices"

func Transpose(graph [][]int) [][]int {
	transposed := make([][]int, len(graph))
	for u, vs := range graph {
		for _, v := range vs {
			transposed[v] = append(transposed[v], u)
		}
	}
	for v := range transposed {
		slices.Sort(transposed[v])
	}
	return transposed
}
