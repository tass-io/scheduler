package initial

import (
	"archive/zip"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	"github.com/tass-io/scheduler/pkg/store"
	"github.com/tass-io/scheduler/pkg/tools/k8sutils"
	"go.uber.org/zap"
)

// this package will handle function process init and exec specific function
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

// functionInit will move function code to it's own directory
// redirect stdout and stderr to file
// and exec function code
func functionInit(funcName string) {
	codeBase64, err := store.Get(k8sutils.GetSelfNamespace(), funcName)
	if err != nil {
		panic(err)
	}
	codePrepareAndExec(codeBase64)
}

func codePrepareAndExec(code string) {
	codePrepare(code)
	// todo support customize cmd
	pid := os.Getpid()
	directoryPath := fmt.Sprintf("/tmp/tass/%d", pid)
	codePath := directoryPath + "/code"
	entryPath := codePath + "/index.js"
	if err := syscall.Exec("node", []string{entryPath}, os.Environ()); err != nil {
		zap.S().Errorw("init exec error", "err", err)
	}
}

func codePrepare(code string) {
	pid := os.Getpid()
	directoryPath := fmt.Sprintf("/tmp/tass/%d", pid)
	codePath := directoryPath + "/code"
	codeZipPath := codePath + "/code.zip"
	err := os.MkdirAll(directoryPath, 0775)
	if err != nil {
		panic(err)
	}

	err = os.Mkdir(codePath, 0775)
	if err != nil {
		panic(err)
	}

	if err != nil {
		panic(err)
	}

	dec, err := base64.StdEncoding.DecodeString(code)
	if err != nil {
		zap.S().Errorw("init base64 decode error", "err", err)
		panic(err)
	}
	f, err := os.Create(codeZipPath)
	if err != nil {
		panic(err)
	}
	if _, err := f.Write(dec); err != nil {
		zap.S().Errorw("init write error", "err", err)
		panic(err)
	}
	if err := f.Sync(); err != nil {
		zap.S().Errorw("init sync error", "err", err)
		panic(err)
	}
	_ = f.Close()
	filepaths, err := unzip(codeZipPath, codePath)
	if err != nil {
		zap.S().Errorw("init unzip error", "err", err)
		panic(err)
	}
	zap.S().Infow("unzip user code", "filepath", filepaths)
}

// unzip will decompress a zip archive, moving all files and folders
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
	InitCmd.Flags().StringP(env.RedisIp, "I", "10.0.0.96", "redis ip to init function")
	viper.BindPFlag(env.RedisIp, InitCmd.Flags().Lookup(env.RedisIp))
	InitCmd.Flags().StringP(env.RedisPort, "P", "30285", "redis port to init function")
	viper.BindPFlag(env.RedisPort, InitCmd.Flags().Lookup(env.RedisPort))
	InitCmd.Flags().StringP(env.RedisPassword, "S", "", "redis password to init function")
	viper.BindPFlag(env.RedisPassword, InitCmd.Flags().Lookup(env.RedisPassword))
	InitCmd.Flags().Int32P(env.DefaultDb, "D", 0, "redis default db to init function")
	viper.BindPFlag(env.DefaultDb, InitCmd.Flags().Lookup(env.DefaultDb))
}
