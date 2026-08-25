package utils

func Filter[T any](input []T, predicate func(T) bool) []T {
	output := make([]T, 0, len(input))
	for _, item := range input {
		if predicate(item) {
			output = append(output, item)
		}
	}
	return output
}

func Map[T any, U any](input []T, mapper func(T) (U, error)) ([]U, error) {
	output := make([]U, 0, len(input))
	for _, item := range input {
		mappedItem, err := mapper(item)
		if err != nil {
			return nil, err
		}
		output = append(output, mappedItem)
	}
	return output, nil
}
