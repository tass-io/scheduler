package common

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
)

// obj must be a pointer
func GetValue(body map[string]interface{}, target string, obj interface{}) {
	jsonString, _ := json.Marshal(body)
	if needCheckObj := strings.HasPrefix(target, "$."); needCheckObj {
		realTarget := strings.TrimPrefix(target, "$.")
		val := gjson.Get(string(jsonString), realTarget)

		switch obj.(type) {
		case *int:
			{
				intval := obj.(*int)
				*intval = int(val.Int())
			}
		case *string:
			{
				strval := obj.(*string)
				*strval = val.String()
			}
		case *bool:
			{
				boolval := obj.(*bool)
				*boolval = val.Bool()
			}
		default:
			panic(obj)
		}
	} else {
		// constant
		switch obj.(type) {
		case *int:
			{
				intval := obj.(*int)
				*intval, _ = strconv.Atoi(target)
			}
		case *string:
			{
				strval := obj.(*string)
				*strval = target
			}
		case *bool:
			{
				boolval := obj.(*bool)
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
