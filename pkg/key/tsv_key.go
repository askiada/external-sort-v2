package key

import (
	"fmt"
	"strings"

	"github.com/askiada/external-sort-v2/pkg/model"
)

const salt = "##!##"

func AllocateCsv(row interface{}, pos ...int) (model.Key, error) {
	splitted, ok := row.([]string)
	if !ok {
		return nil, fmt.Errorf("can't convert interface{} to []string: %+v", row)
	}

	strBuilder := strings.Builder{}

	for i, p := range pos {
		if p >= len(splitted) {
			return nil, fmt.Errorf("position %d is out of range", p)
		}

		_, err := strBuilder.WriteString(splitted[p])
		if err != nil {
			return nil, fmt.Errorf("failed to write string: %w", err)
		}

		if i < len(pos)-1 {
			strBuilder.WriteString(salt)
		}
	}

	return &String{strBuilder.String()}, nil
}
