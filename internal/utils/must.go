package utils

func Must[T any](value T, err error) T {
	if err != nil {
		panic(err)
	}
	return value
}

func MustDefault[T any](value T, err error, defaultValue T) T {
	if err != nil {
		return defaultValue
	}
	return value
}
