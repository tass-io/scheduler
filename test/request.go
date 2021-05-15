package test

import (
	"encoding/json"
	"go.uber.org/zap"
	"io/ioutil"
	"net/http"
	"strings"
)

// wrapper request to local scheduler
func RequestJson(url string, method string, headers map[string]string, param interface{}, bodyStruct interface{}) int {
	var jsonByte []byte
	jsonByte, _ = json.Marshal(param)
	client := &http.Client{
	}
	req, err := http.NewRequest(method, url, strings.NewReader(string(jsonByte)))

	if err != nil {
		panic(err)
	}
	req.Header.Add("Content-Type", "application/json")
	for key, val := range headers {
		req.Header.Add(key, val)
	}

	res, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}
	zap.S().Debugw("get body", "body", string(body))
	err = json.Unmarshal(body, bodyStruct)
	if err != nil {
		panic(err)
	}
	return res.StatusCode
}
