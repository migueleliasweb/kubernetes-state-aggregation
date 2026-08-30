package main

import (
	"bytes"
	"testing"
)

func TestCobraRootCmdFlags(t *testing.T) {
	cmd := newRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--help"})

	err := cmd.Execute()
	if err != nil {
		t.Fatalf("expected help command to execute cleanly, got: %v", err)
	}

	output := buf.String()
	flags := []string{
		"--config",
		"--cluster",
		"--db-url",
		"--log-level",
		"--listen-addr",
		"--enable-sync",
		"--enable-api",
	}

	for _, flag := range flags {
		if !bytes.Contains([]byte(output), []byte(flag)) {
			t.Errorf("expected help output to contain %s flag", flag)
		}
	}
}
