package main

import (
	"bytes"
	"testing"
)

func TestKsadevRootCmd(t *testing.T) {
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

	if !bytes.Contains([]byte(output), []byte("--log-level")) {
		t.Errorf("expected help output to contain --log-level flag")
	}

	if !bytes.Contains([]byte(output), []byte("testcluster")) {
		t.Errorf("expected help output to contain testcluster subcommand")
	}
}

func TestKsadevTestclusterSubcommands(t *testing.T) {
	cmd := newRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"testcluster", "--help"})

	err := cmd.Execute()
	if err != nil {
		t.Fatalf("expected testcluster help to execute cleanly, got: %v", err)
	}

	output := buf.String()

	subcommands := []string{"up", "down"}
	for _, sub := range subcommands {
		if !bytes.Contains([]byte(output), []byte(sub)) {
			t.Errorf("expected testcluster help to contain %s subcommand", sub)
		}
	}
}

func TestKsadevTestclusterUpFlags(t *testing.T) {
	cmd := newRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"testcluster", "up", "--help"})

	err := cmd.Execute()
	if err != nil {
		t.Fatalf("expected testcluster up help to execute cleanly, got: %v", err)
	}

	output := buf.String()

	flags := []string{
		"--clusters",
		"--seed",
		"--scale",
		"--output-config",
		"--config",
		"--clean",
	}

	for _, flag := range flags {
		if !bytes.Contains([]byte(output), []byte(flag)) {
			t.Errorf("expected testcluster up help to contain %s flag", flag)
		}
	}
}

func TestKsadevTestclusterDownFlags(t *testing.T) {
	cmd := newRootCmd()
	buf := new(bytes.Buffer)
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"testcluster", "down", "--help"})

	err := cmd.Execute()
	if err != nil {
		t.Fatalf("expected testcluster down help to execute cleanly, got: %v", err)
	}

	output := buf.String()

	flags := []string{
		"--clusters",
		"--output-config",
		"--config",
		"--all",
	}

	for _, flag := range flags {
		if !bytes.Contains([]byte(output), []byte(flag)) {
			t.Errorf("expected testcluster down help to contain %s flag", flag)
		}
	}
}
