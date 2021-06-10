package initial

import (
	"archive/zip"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	"github.com/tass-io/scheduler/pkg/store"
	"github.com/tass-io/scheduler/pkg/tools/k8sutils"
	_ "github.com/tass-io/scheduler/pkg/tools/log"
	"go.uber.org/zap"
)

// initial pkg handles function process initialization and executes the specific function
var (
	funcName string
	InitCmd  = &cobra.Command{
		Use:   "init",
		Short: "init a function process",
		Long:  "init a function process",
		Run: func(cmd *cobra.Command, args []string) {
			functionInit(funcName)
		},
	}
)

// functionInit moves function code to it's own directory,
// redirects stdout and stderr to file and executes function code
func functionInit(functionName string) {
	codeBase64, err := store.Get(k8sutils.GetSelfNamespace(), functionName)
	if err != nil {
		zap.S().Panicw("get function content error", "err", err)
	}
	codePrepareAndExec(codeBase64, functionName, viper.GetString(env.Environment))
}

// codePrepareAndExec prepares code and executes it
func codePrepareAndExec(code string, functionName string, environment string) {
	codePrepare(code)
	codeExec(functionName, environment)
}

// codePrepare decodes & unzips the code and places the code to the desired location
func codePrepare(code string) {
	pid := os.Getpid()
	directoryPath := fmt.Sprintf(env.TassFileRoot+"%d", pid)
	// clean up first
	os.RemoveAll(directoryPath)
	codePath := directoryPath + "/code"
	codeZipPath := codePath + "/code.zip"
	err := os.MkdirAll(directoryPath, 0777)
	if err != nil {
		zap.S().Panicw("code prepare mkdir all error", "err", err)
	}

	err = os.Mkdir(codePath, 0777)
	if err != nil {
		zap.S().Panicw("code prepare mkdir error", "err", err)
	}

	dec, err := base64.StdEncoding.DecodeString(code)
	if err != nil {
		zap.S().Panicw("init base64 decode error", "err", err)
	}
	f, err := os.Create(codeZipPath)
	if err != nil {
		zap.S().Panicw("code prepare create error", "err", err)
	}
	if _, err := f.Write(dec); err != nil {
		zap.S().Panicw("init write error", "err", err)
	}
	if err := f.Sync(); err != nil {
		zap.S().Panicw("init sync error", "err", err)
	}
	_ = f.Close()
	filepaths, err := unzip(codeZipPath, codePath)
	if err != nil {
		zap.S().Panicw("init unzip error", "err", err)
	}
	zap.S().Infow("unzip user code", "filepath", filepaths)
}

// codeExec executes the prepared code
func codeExec(functionName string, environment string) {
	// todo support customize cmd
	pid := os.Getpid()
	directoryPath := fmt.Sprintf(env.TassFileRoot+"%d", pid)
	codePath := directoryPath + "/code"
	switch environment {
	case "JavaScript":
		{
			entryPath := codePath + "/index.js"
			zap.S().Debugw("run with entryPath", "path", entryPath)
			if _, err := os.Stat(entryPath); err != nil {
				zap.S().Errorw("code file error", "err", err, "entryPath", entryPath)
			}
			binary, err := exec.LookPath("node")
			if err != nil {
				zap.S().Errorw("environment prepare error at JavaScript", "err", err)
				os.Exit(2)
			}
			if err := syscall.Exec(binary, []string{entryPath}, os.Environ()); err != nil {
				zap.S().Errorw("init exec error", "err", err)
				os.Exit(2)
			}
		}
	case "Golang":
		{
			entryPath := codePath + "/main"
			pluginPath := codePath + "/plugin.so"
			err := os.Chmod(entryPath, 0777)
			if err != nil {
				zap.S().Errorw("init chmod error", "err", err)
				os.Exit(3)
			}
			zap.S().Debugw("prepare to exec golang binary", "entryPath", entryPath)
			// todo support cmd params customize
			if err := syscall.Exec(entryPath, []string{"main", pluginPath}, os.Environ()); err != nil {
				zap.S().Errorw("init exec error", "err", err)
				os.Exit(4)
			}
		}
	default:
		{
			zap.S().Error("init exec with unsupport environment")
			os.Exit(5)
		}
	}
}

// unzip decompresses a zip archive, moving all files and folders
// within the zip file (parameter 1) to an output directory (parameter 2).
func unzip(src string, dest string) ([]string, error) {

	var filenames []string

	r, err := zip.OpenReader(src)
	if err != nil {
		return filenames, err
	}
	defer r.Close()

	for _, f := range r.File {

		err := func() error {
			// Store filename/path for returning and using later on
			fpath := filepath.Join(dest, f.Name)

			// Check for ZipSlip. More Info: http://bit.ly/2MsjAWE
			if !strings.HasPrefix(fpath, filepath.Clean(dest)+string(os.PathSeparator)) {
				return fmt.Errorf("%s: illegal file path", fpath)
			}

			filenames = append(filenames, fpath)

			if f.FileInfo().IsDir() {
				// Make Folder
				os.MkdirAll(fpath, os.ModePerm)
				return nil
			}

			// Make File
			if err = os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
				return err
			}

			outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
			if err != nil {
				return err
			}
			defer outFile.Close()

			rc, err := f.Open()
			if err != nil {
				return err
			}

			_, err = io.Copy(outFile, rc)

			return err

		}()
		if err != nil {
			return filenames, err
		}
	}

	return filenames, nil
}

func init() {
	InitCmd.Flags().StringVarP(&funcName, "name", "n", "", "Name of the function")
	InitCmd.Flags().StringP(env.Environment, "E", "JavaScript", "function run environment/language required")
	viper.BindPFlag(env.Environment, InitCmd.Flags().Lookup(env.Environment))
}
