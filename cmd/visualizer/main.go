package main

import (
	"embed"
	"flag"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"distributed-kv/engine"
)

//go:embed static
var staticFS embed.FS

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dataDir := flag.String("data", "", "data directory (default: temp dir)")
	flag.Parse()

	// Render.com sets PORT env var; override flag if present.
	if port := os.Getenv("PORT"); port != "" {
		p := ":" + port
		addr = &p
	}

	// Default to a temp directory.
	dir := *dataDir
	if dir == "" {
		var err error
		dir, err = os.MkdirTemp("", "lsm-viz-*")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
			os.Exit(1)
		}
		defer os.RemoveAll(dir)
		fmt.Printf("Using temp data dir: %s\n", dir)
	}

	// Create a demo LSMTree with small thresholds for visualization.
	tree, err := engine.OpenLSMTree(dir,
		engine.WithMemTableLimit(512),
		engine.WithL0CompactionThreshold(4),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open LSM tree: %v\n", err)
		os.Exit(1)
	}

	bc := NewBroadcaster()
	tree.SetObserver(bc)

	handler := NewHandler(tree, bc, dir)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	// Serve static files (index.html).
	staticSub, err := fs.Sub(staticFS, "static")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create sub fs: %v\n", err)
		os.Exit(1)
	}
	mux.Handle("/", http.FileServer(http.FS(staticSub)))

	server := &http.Server{Addr: *addr, Handler: mux}

	go func() {
		fmt.Printf("LSM-Tree Visualizer running at http://localhost%s\n", *addr)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "server error: %v\n", err)
			os.Exit(1)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	fmt.Println("\nShutting down...")
	server.Close()
	tree.Close()
	fmt.Println("Done.")
}
