package run

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

var Host = "LOCAL"

type StderrError string

func (self StderrError) Error() string {
	return string(self)
}

func Start(path string, args ...string) (result chan error) {
	return startAndLog("", path, args...)
}

func StartAndLog(logfile string, path string, args ...string) (result chan error) {
	return startAndLog(logfile, path, args...)
}

func startAndLog(logfile string, path string, args ...string) (result chan error) {
	result = make(chan error, 1)

	var file *os.File
	var err error
	if logfile != "" {
		if file, err = os.Create(logfile); err != nil {
			fmt.Printf("Could not create logfile %#v", logfile)
			result <- err
			return
		}
	}

	fmt.Printf(" ( *** %v ) %v", Host, path)
	for _, bit := range args {
		fmt.Printf(" %#v", bit)
	}
	if logfile != "" {
		fmt.Printf(" > %#v\n", logfile)
	} else {
		fmt.Printf("\n")
	}

	cmd := exec.Command(path, args...)

	cmd.Stdin = os.Stdin
	if logfile != "" {
		cmd.Stdout, cmd.Stderr = file, file
	} else {
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	}
	if err = cmd.Start(); err != nil {
		result <- err
		return
	}

	go func() {
		if logfile != "" {
			defer file.Close()
		}
		result <- cmd.Wait()
	}()

	return
}

func RunAndReturn(path string, params ...string) (stdout, stderr string, err error) {
	fmt.Printf(" ( *** %v ) %v", Host, path)
	for _, bit := range params {
		fmt.Printf(" %#v", bit)
	}
	fmt.Println("")

	cmd := exec.Command(path, params...)
	o := new(bytes.Buffer)
	e := new(bytes.Buffer)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, o, e
	err = cmd.Run()
	stdout, stderr = o.String(), e.String()
	return
}

func RunSilent(path string, params ...string) (err error) {
	return run(true, path, params...)
}

func Run(path string, params ...string) (err error) {
	return run(false, path, params...)
}

func run(silent bool, path string, params ...string) (err error) {
	cmd := exec.Command(path, params...)
	buf := new(bytes.Buffer)
	if silent {
		cmd.Stderr = buf
	} else {
		cmd.Stderr = io.MultiWriter(buf, os.Stderr)
		cmd.Stdin, cmd.Stdout = os.Stdin, os.Stdout
		fmt.Printf(" ( *** %v ) %v", Host, path)
		for _, bit := range params {
			fmt.Printf(" %#v", bit)
		}
		fmt.Println("")
	}
	err = cmd.Run()
	if strings.TrimSpace(string(buf.Bytes())) != "" {
		err = StderrError(buf.String())
		return
	}
	if err != nil {
		return
	}
	return
}
