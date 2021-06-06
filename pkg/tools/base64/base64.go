package base64

import (
	"bufio"
	"encoding/base64"
	"io/ioutil"
	"os"
)

// EncodeUserCode encodes the user code into base64
func EncodeUserCode(name string) (string, error) {
	f, err := os.Open(name)
	if err != nil {
		return "", err
	}
	// Read entire file into byte slice.
	reader := bufio.NewReader(f)
	content, err := ioutil.ReadAll(reader)
	if err != nil {
		return "", err
	}
	// Encode as base64.
	encoded := base64.StdEncoding.EncodeToString(content)
	return encoded, nil
}
