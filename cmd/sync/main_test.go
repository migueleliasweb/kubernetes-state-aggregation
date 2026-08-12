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
	if !bytes.Contains([]byte(output), []byte("--config")) {
		t.Errorf("expected help output to contain --config flag")
	}
	if !bytes.Contains([]byte(output), []byte("--cluster")) {
		t.Errorf("expected help output to contain --cluster flag")
	}
	if !bytes.Contains([]byte(output), []byte("--db-url")) {
		t.Errorf("expected help output to contain --db-url flag")
	}
}
