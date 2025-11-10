package parser

func Fuzz(data []byte) int {
	if _, _, err := ParseExpr(string(data)); err != nil {
		return 0
	}
	return 1
}
