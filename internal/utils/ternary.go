package utils

func Ternary[T any](cond bool, valueIfTrue T, valueIfFalse T) T {
	if cond {
		return valueIfTrue
	} else {
		return valueIfFalse
	}
}
