package initial_test

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	"github.com/tass-io/scheduler/pkg/store"
	_ "github.com/tass-io/scheduler/pkg/tools/log"
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

// InitCmd use binary level black test because of syscall.Exec
func TestInitCmd(t *testing.T) {
	Convey("test init cmd", t, func() {
		testcases := []struct {
			caseName     string
			skipped      bool
			functionName string
			environment  string
			fileName     string
			expect       string
		}{
			{
				caseName:     "hello world test",
				skipped:      false,
				functionName: "hello",
				environment:  "JavaScript",
				fileName:     "../../user-code/default-hello.zip",
			},
		}
		// build binary
		complieCmd := exec.Command("go", "build", "../../main.go")
		err := complieCmd.Start()
		So(err, ShouldBeNil)
		err = complieCmd.Wait()
		So(err, ShouldBeNil)
		// use Cmd to run init cmd
		for _, testcase := range testcases {
			if testcase.skipped {
				continue
			}
			t.Log(testcase.caseName)
			// prepare code file
			code, err := encodeUserCode(testcase.fileName)
			So(err, ShouldBeNil)
			viper.Set(env.RedisIp, "10.0.0.96")
			viper.Set(env.RedisPort, "30285")
			viper.Set(env.RedisPassword, "")
			viper.Set(env.DefaultDb, 0)
			err = store.Set("default", testcase.functionName, code)
			So(err, ShouldBeNil)
			initParam := fmt.Sprintf("init -n %s -I 10.0.0.96 -P 30285 -D 0 -E %s", testcase.functionName, testcase.environment)
			initCmd := exec.Command("./main", strings.Split(initParam, " ")...)
			output, err := initCmd.Output()
			t.Logf("process output %s\n", string(output))
			So(err, ShouldBeNil)
			So(string(output), ShouldContainSubstring, testcase.expect)
		}
		// get the result of it from stdout
	})

}
