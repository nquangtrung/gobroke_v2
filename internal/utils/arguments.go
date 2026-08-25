package utils

func indexOf(s string, char byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == char {
			return i
		}
	}
	return -1
}

// NamedArguments parses command-line arguments and returns a map of named arguments.
// Named arguments are expected to be in the format --key=value or --key.
func NamedArguments(args []string) map[string]string {
	namedArgs := make(map[string]string)
	for _, arg := range args {
		if len(arg) > 2 && arg[0:2] == "--" {
			// Split the argument into key and value
			keyValue := arg[2:]
			if equalIndex := indexOf(keyValue, '='); equalIndex != -1 {
				key := keyValue[:equalIndex]
				value := keyValue[equalIndex+1:]
				namedArgs[key] = value
			} else {
				namedArgs[keyValue] = ""
			}
		}
	}
	return namedArgs
}
