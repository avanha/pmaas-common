package slices

func Apply[S any, V any](inputs []S, f func(entry *S) V) []V {
	results := make([]V, len(inputs))

	for i := 0; i < len(inputs); i++ {
		results[i] = f(&inputs[i])
	}

	return results
}
