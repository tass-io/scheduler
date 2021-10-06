package main

// nolint: unused
func Handler(parameters map[string]interface{}) (map[string]interface{}, error) {
	parameters["plugin"] = "plugin"
	return parameters, nil
}
