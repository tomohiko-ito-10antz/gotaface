package topological

func DFS(graph [][]int, root int, visit func(node int) error) error {

	visited := make([]bool, len(graph))
	if err := dfsImpl(&graph, root, visit, &visited); err != nil {
		return err
	}
	return nil
}

func dfsImpl(graph *[][]int, u int, visit func(node int) error, visited *[]bool) error {
	if (*visited)[u] {
		return nil
	}

	(*visited)[u] = true

	if err := visit(u); err != nil {
		return err
	}

	for _, v := range (*graph)[u] {
		if (*visited)[v] {
			continue
		}

		if err := dfsImpl(graph, v, visit, visited); err != nil {
			return err
		}
	}

	return nil
}
