package slices

func Apply[S any, V any](inputs []S, f func(entry *S) V) []V {
	results := make([]V, len(inputs))

	for i := 0; i < len(inputs); i++ {
		results[i] = f(&inputs[i])
	}

	return results
}

func Filter[S any](inputs []S, predicate func(entry *S) bool) []S {
	results := make([]S, 0, len(inputs))

	for i := 0; i < len(inputs); i++ {
		if predicate(&inputs[i]) {
			results = append(results, inputs[i])
		}
	}

	return results
}
