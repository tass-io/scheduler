package errorutils

import (
	"fmt"
)

func NewNoInstanceError(functionName string) error {
	return fmt.Errorf("%s has no instnaces", functionName)
}
