package initial

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/tass-io/scheduler/pkg/env"
	"github.com/tass-io/scheduler/pkg/store"
	"github.com/tass-io/scheduler/pkg/tools/k8sutils"
)

func encodeUserCode(name string) (string, error) {
	f, err := os.Open(name)
	if err != nil {
		return "", err
	}
	// Read entire JPG into byte slice.
	reader := bufio.NewReader(f)
	content, err := ioutil.ReadAll(reader)
	if err != nil {
		return "", err
	}
	// Encode as base64.
	encoded := base64.StdEncoding.EncodeToString(content)
	return encoded, nil
}

func TestFunctionFilePrepare(t *testing.T) {
	Convey("test function Init", t, func() {
		store.Get = func(ns, name string) (string, error) {
			return encodeUserCode(fmt.Sprintf("../../user-code/%s-%s.zip", ns, name))
		}
		testcases := []struct {
			caseName string
			name     string
			expect   string
		}{
			{
				caseName: "test unzip single file",
				name:     "hello",
				expect:   "hello world\n",
			},
			{
				caseName: "test unzip zip file with folder",
				name:     "folder",
				expect:   "test in tool\n",
			},
		}
		for _, testcase := range testcases {
			Convey(testcase.caseName, func() {
				t.Log(testcase.caseName)
				code, err := store.Get(k8sutils.GetSelfNamespace(), testcase.name)
				So(err, ShouldBeNil)
				// test code
				codePrepare(code)
				pid := os.Getpid()
				directoryPath := fmt.Sprintf(env.TassFileRoot+"%d", pid)
				codePath := directoryPath + "/code"
				indexFile := codePath + "/index.js"
				newProcess := exec.Command("node", indexFile)
				b := bytes.NewBuffer(make([]byte, 0))
				output := bufio.NewWriter(b)
				newProcess.Stdout = output
				err = newProcess.Run()
				So(err, ShouldBeNil)
				So(b.String(), ShouldEqual, testcase.expect)
				// clean up code
				err = os.RemoveAll(directoryPath)
				So(err, ShouldBeNil)
			})
		}
	})
}
