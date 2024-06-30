package key

import (
	"fmt"
	"strings"
)

const salt = "##!##"

func AllocateTsv(row interface{}, pos ...int) (Key, error) {
	splitted, ok := row.([]string)
	if !ok {
		return nil, fmt.Errorf("can't convert interface{} to []string: %+v", row)
	}

	strBuilder := strings.Builder{}

	for i, p := range pos {
		if len(splitted) < p+1 {
			return nil, fmt.Errorf("position %d is out of range", p)
		}

		strBuilder.WriteString(fmt.Sprint(splitted[p]))

		if i < len(pos)-1 {
			strBuilder.WriteString(salt)
		}
	}

	return &String{strBuilder.String()}, nil
}
