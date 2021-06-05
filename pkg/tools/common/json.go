package common

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
)

// GetValue returns the value of the body by target
// the input obj must be a pointer
func GetValue(body map[string]interface{}, target string, obj interface{}) {
	jsonString, _ := json.Marshal(body)
	if needCheckObj := strings.HasPrefix(target, "$."); needCheckObj {
		realTarget := strings.TrimPrefix(target, "$.")
		val := gjson.Get(string(jsonString), realTarget)

		switch obj := obj.(type) {
		case *int:
			{
				intval := obj
				*intval = int(val.Int())
			}
		case *string:
			{
				strval := obj
				*strval = val.String()
			}
		case *bool:
			{
				boolval := obj
				*boolval = val.Bool()
			}
		default:
			panic(obj)
		}
	} else {
		// constant
		switch obj := obj.(type) {
		case *int:
			{
				intval := obj
				*intval, _ = strconv.Atoi(target)
			}
		case *string:
			{
				strval := obj
				*strval = target
			}
		case *bool:
			{
				boolval := obj
				if target == "True" || target == "true" || target == "1" {
					*boolval = true
				} else {
					*boolval = false
				}
			}
		default:
			panic(obj)
		}
	}

}
