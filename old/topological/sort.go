package topological

import "golang.org/x/exp/slices"

// Sort performs topological sort on a directed graph represented by the adjacency list.
// It takes a 2D integer array 'graph' where graph[u] contains the indices of the destinations
// that can be reached from vertex u. This function returns the sorted vertices
// and a boolean flag indicating whether a valid topological order exists.
//
// If a valid topological order exists, this function returns (orders for each vertex, true).
// If a cycle is detected in the graph, this function returns (nil, false).
//
// Example:
// order, ok := topological.Sort([][]int{{5}, {3, 6}, {5, 7}, {0, 7}, {1, 2, 6}, {}, {7}, {0}})
// println(order, ok) // []int{4, 1, 1, 2, 0, 5, 2, 3} true
//
// order, ok := topological.Sort([][]int{{1}, {0}})
// println(order, ok) // nil false
func Sort(graph [][]int) ([]int, bool) {
	// count in-degrees
	inDegrees := make([]int, len(graph))
	for u := range graph {
		inDegrees[u] = 0
	}
	for _, vs := range graph {
		for _, v := range vs {
			inDegrees[v]++
		}
	}

	// init queue
	queue := []int{}
	order := make([]int, len(graph))
	ord := 0

	queueHead := 0
	for u := range graph {
		order[u] = -1
		if inDegrees[u] == 0 {
			order[u] = ord
			queue = append(queue, u)
		}
	}

	// perform topological sort
	for queueHead < len(queue) {
		u := queue[queueHead]
		queueHead++
		inclementOrd := false
		for _, v := range graph[u] {
			inDegrees[v]--
			if inDegrees[v] == 0 {
				inclementOrd = true
			}
		}
		if inclementOrd {
			ord++
		}
		for _, v := range graph[u] {
			if inDegrees[v] == 0 {
				order[v] = ord
				queue = append(queue, v)
			}
		}
	}

	if slices.Contains(order, -1) { // cycle detected
		return nil, false
	}

	// no cycles
	return order, true
}
