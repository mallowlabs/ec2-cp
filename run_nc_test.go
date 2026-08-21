package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// renderBootScript renders the real boot script with tncl installed into dir instead of /tmp.
func renderBootScript(t *testing.T, dir string) string {
	t.Helper()

	buf := &strings.Builder{}
	err := bootAgentTmpl.Execute(buf, bootAgentTmplData{
		Cmd: filepath.Join(dir, "tncl"), Port: 20000, Filename: filepath.Join(dir, "out"),
		URLTmpl:       "https://github.com/fujiwara/tncl/releases/download/" + tnclVersion + "/tncl-$arch-linux-musl",
		ReadyEchoExpr: echoQuoted(readyMarker), SetupFailedEchoExpr: echoQuoted(setupFailedMarker),
		DoneEchoExpr: echoQuoted(doneMarker)})
	if err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

// setupScript carves the tncl installation out of the full boot script: everything between the
// `cd` into root's home and the line that actually launches tncl. Running the rest would need a
// real instance (`sudo su`) and would block forever on `sleep infinity`.
func setupScript(t *testing.T, script string, cmd string) string {
	t.Helper()

	var kept []string
	started := false
	for _, line := range strings.Split(script, "\n") {
		if strings.HasPrefix(line, "cd ") {
			started = true
			continue
		}
		if !started {
			continue
		}
		if strings.HasPrefix(line, cmd+" ") {
			return strings.Join(kept, "\n")
		}
		kept = append(kept, line)
	}

	t.Fatalf("no line launching %q found in boot script:\n%s", cmd, script)
	return ""
}

// stubTool writes an executable named name into dir, shadowing the real tool for the duration of
// the test.
func stubTool(t *testing.T, dir, name, body string) {
	t.Helper()

	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0o755); err != nil {
		t.Fatal(err)
	}
}

// runSetup runs the boot script's installation step with uname reporting arch and curl exiting
// with curlExit, and reports what the script printed plus the arguments curl was called with.
func runSetup(t *testing.T, arch string, curlExit int) (output string, curlArgs string) {
	t.Helper()

	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	argsFile := filepath.Join(dir, "curl-args")

	stubTool(t, bin, "uname", "echo "+arch+"\n")
	// Mimics curl closely enough to matter: it still creates the -o file even when it fails, which
	// is exactly why a failure has to be noticed here rather than at chmod/exec time.
	stubTool(t, bin, "curl", `printf '%s\n' "$*" > `+argsFile+`
while [ $# -gt 0 ]; do
  if [ "$1" = "-o" ]; then shift; : > "$1"; fi
  shift
done
exit `+string(rune('0'+curlExit))+"\n")

	script := setupScript(t, renderBootScript(t, dir), filepath.Join(dir, "tncl"))

	cmd := exec.Command("bash", "-c", script)
	cmd.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("running setup script: %v\n%s", err, out)
	}

	args, err := os.ReadFile(argsFile)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return string(out), string(args)
}

func TestBootScriptDownloadsMatchingArchitecture(t *testing.T) {
	for _, arch := range []string{"x86_64", "aarch64"} {
		t.Run(arch, func(t *testing.T) {
			out, curlArgs := runSetup(t, arch, 0)

			if !strings.Contains(out, readyMarker) {
				t.Errorf("expected the ready marker, got:\n%s", out)
			}
			want := "tncl-" + arch + "-linux-musl"
			if !strings.Contains(curlArgs, want) {
				t.Errorf("expected curl to download %q, was called with: %s", want, curlArgs)
			}
		})
	}
}

func TestBootScriptRejectsUnsupportedArchitecture(t *testing.T) {
	out, curlArgs := runSetup(t, "armv7l", 0)

	if !strings.Contains(out, setupFailedMarker) {
		t.Errorf("expected the setup failed marker, got:\n%s", out)
	}
	if strings.Contains(out, readyMarker) {
		t.Errorf("expected no ready marker, got:\n%s", out)
	}
	if curlArgs != "" {
		t.Errorf("expected no download to be attempted, curl was called with: %s", curlArgs)
	}
}

func TestBootScriptReportsFailedDownload(t *testing.T) {
	out, _ := runSetup(t, "x86_64", 7)

	if !strings.Contains(out, setupFailedMarker) {
		t.Errorf("expected the setup failed marker, got:\n%s", out)
	}
	if strings.Contains(out, readyMarker) {
		t.Errorf("expected no ready marker, got:\n%s", out)
	}
}

// The markers double as "this really was printed by the remote, not just echoed back as the input
// we typed" signals, so the script that is typed must never contain one intact.
func TestBootScriptNeverContainsMarkersIntact(t *testing.T) {
	script := renderBootScript(t, t.TempDir())

	for _, marker := range []string{readyMarker, setupFailedMarker, doneMarker} {
		if strings.Contains(script, marker) {
			t.Errorf("boot script contains %q intact:\n%s", marker, script)
		}
	}
}
