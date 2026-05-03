package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"testing"
	"time"

	"github.com/rogpeppe/go-internal/testscript"
)

func TestMain(m *testing.M) {
	testscript.Main(m, map[string]func(){
		"t4": runT4Main,
	})
}

func runT4Main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	rootCmd.SetOut(os.Stdout)
	rootCmd.SetErr(os.Stderr)
	rootCmd.SetArgs(os.Args[1:])
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		os.Exit(1)
	}
}

func TestScripts(t *testing.T) {
	ctx := t.Context()
	testscript.Run(t, testscript.Params{
		Dir: "testdata/script",
		Setup: func(env *testscript.Env) error {
			// One server, two named buckets (mirrors `t4 serve a=... b=...`).
			// Scripts address them via the path-style URI form
			// `<server>/<name>//<key>`.
			dirA := filepath.Join(env.WorkDir, "_storage_a")
			dirB := filepath.Join(env.WorkDir, "_storage_b")
			for _, d := range []string{dirA, dirB} {
				if err := os.MkdirAll(d, 0o755); err != nil {
					return err
				}
			}
			handler, err := buildBucketMux(ctx, []bucketSpec{
				{name: "a", uri: "file://" + dirA},
				{name: "b", uri: "file://" + dirB},
			})
			if err != nil {
				return err
			}
			server := httptest.NewServer(handler)
			env.Defer(server.Close)
			env.Setenv("BASE_URL", server.URL)
			env.Setenv("BUCKET_A", server.URL+"/a/")
			env.Setenv("BUCKET_B", server.URL+"/b/")
			env.Setenv("STORAGE_A", dirA)
			env.Setenv("STORAGE_B", dirB)
			return nil
		},
		Cmds: map[string]func(*testscript.TestScript, bool, []string){
			"pickport": pickPortCmd,
			"ready":    readyCmd,
		},
	})
}

func pickPortCmd(ts *testscript.TestScript, neg bool, args []string) {
	if neg {
		ts.Fatalf("pickport: unexpected negation")
	}
	if len(args) != 1 {
		ts.Fatalf("usage: pickport ENVVAR")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		ts.Fatalf("pickport listen: %v", err)
	}
	addr := l.Addr().(*net.TCPAddr).String()
	l.Close()
	ts.Setenv(args[0], addr)
}

func readyCmd(ts *testscript.TestScript, neg bool, args []string) {
	if len(args) != 1 {
		ts.Fatalf("usage: ready URL")
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		res, err := http.Get(args[0])
		if err == nil {
			res.Body.Close()
			ok := res.StatusCode < 500
			if neg && !ok {
				return
			}
			if !neg && ok {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	if neg {
		ts.Fatalf("ready: %s kept responding", args[0])
	}
	ts.Fatalf("ready: timed out waiting for %s", args[0])
}
