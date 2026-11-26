// Command nodestatd reports the state of a Kubernetes node. Its images module
// records the images in the node's containerd or CRI-O image store as
// ContainerImage objects.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/codemowers/nodestatd/internal/containerd"
	"github.com/codemowers/nodestatd/internal/crio"
	"github.com/codemowers/nodestatd/internal/images"
	"github.com/codemowers/nodestatd/internal/server"
	"golang.org/x/sync/errgroup"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type config struct {
	runtime             string
	containerdAddress   string
	containerdNamespace string
	crioAddress         string
	crioStorageRoot     string
	crioStorageDriver   string
	node                string
	kubeconfig          string
	healthAddr          string
	logLevel            string
	enableImages        bool
	images              images.Config
}

func main() {
	cfg := parseConfig()
	log := newLogger(cfg.logLevel)
	if cfg.node == "" {
		log.Error("NODE_NAME is required")
		os.Exit(2)
	}
	if cfg.runtime != "containerd" && cfg.runtime != "crio" {
		log.Error("CONTAINER_RUNTIME must be containerd or crio", "runtime", cfg.runtime)
		os.Exit(2)
	}
	if !cfg.enableImages {
		log.Error("no module enabled")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("nodestatd stopped", "error", err)
		os.Exit(1)
	}
}

func parseConfig() config {
	var cfg config
	flag.StringVar(&cfg.runtime, "container-runtime", env("CONTAINER_RUNTIME", "containerd"), "container runtime: containerd or crio")
	flag.StringVar(&cfg.containerdAddress, "containerd-address", env("CONTAINERD_ADDRESS", "/run/containerd/containerd.sock"), "containerd socket")
	flag.StringVar(&cfg.containerdNamespace, "containerd-namespace", env("CONTAINERD_NAMESPACE", "k8s.io"), "containerd namespace holding Kubernetes containers and images")
	flag.StringVar(&cfg.crioAddress, "crio-address", env("CRIO_ADDRESS", "/run/crio/crio.sock"), "CRI-O socket")
	flag.StringVar(&cfg.crioStorageRoot, "crio-storage-root", env("CRIO_STORAGE_ROOT", "/var/lib/containers/storage"), "containers/storage graph root CRI-O pulls images into")
	flag.StringVar(&cfg.crioStorageDriver, "crio-storage-driver", env("CRIO_STORAGE_DRIVER", "overlay"), "containers/storage driver CRI-O uses")
	flag.StringVar(&cfg.node, "node-name", env("NODE_NAME", ""), "Kubernetes node name")
	flag.StringVar(&cfg.kubeconfig, "kubeconfig", env("KUBECONFIG", ""), "kubeconfig path; in-cluster configuration when empty")
	flag.StringVar(&cfg.healthAddr, "health-addr", env("HEALTH_ADDR", ":5281"), "health endpoint address")
	flag.StringVar(&cfg.logLevel, "log-level", env("LOG_LEVEL", "info"), "debug, info, warn or error")
	flag.BoolVar(&cfg.enableImages, "enable-images", envBool("ENABLE_IMAGES", true), "record images as ContainerImage objects")
	flag.DurationVar(&cfg.images.ResyncInterval, "images-resync-interval", envDuration("IMAGES_RESYNC_INTERVAL", time.Hour), "full image reconciliation interval")
	flag.DurationVar(&cfg.images.Debounce, "images-event-debounce", envDuration("IMAGES_EVENT_DEBOUNCE", 5*time.Second), "delay before syncing images after runtime events")
	flag.BoolVar(&cfg.images.RecordEnv, "images-record-env", envBool("IMAGES_RECORD_ENV", false), "record image config environment variables")
	flag.Parse()
	return cfg
}

func run(ctx context.Context, cfg config, log *slog.Logger) error {
	runtime, healthy, closeRuntime, err := connectRuntime(cfg, log)
	if err != nil {
		return err
	}
	defer closeRuntime()
	live := []server.Check{healthy}
	var ready []server.Check

	group, ctx := errgroup.WithContext(ctx)
	if cfg.enableImages {
		restConfig, err := kubeConfig(cfg.kubeconfig)
		if err != nil {
			return fmt.Errorf("kubernetes config: %w", err)
		}
		dynamicClient, err := dynamic.NewForConfig(restConfig)
		if err != nil {
			return err
		}
		metadataClient, err := metadata.NewForConfig(restConfig)
		if err != nil {
			return err
		}
		module := images.New(cfg.images, runtime, dynamicClient, metadataClient, cfg.node, log)
		ready = append(ready, module.Ready)
		group.Go(func() error { return module.Run(ctx) })
	}
	group.Go(func() error { return server.ServeHealth(ctx, cfg.healthAddr, live, ready, log) })
	return group.Wait()
}

// connectRuntime connects to the configured container runtime, returning the
// image source for the images module and the runtime's health check.
func connectRuntime(cfg config, log *slog.Logger) (images.Runtime, server.Check, func() error, error) {
	if cfg.runtime == "crio" {
		storage := &crio.Storage{Root: cfg.crioStorageRoot, Driver: cfg.crioStorageDriver}
		client, err := crio.Connect(cfg.crioAddress, storage, log)
		if err != nil {
			return nil, nil, nil, err
		}
		return images.CRIO(client), client.Healthy, client.Close, nil
	}
	client, err := containerd.Connect(cfg.containerdAddress, cfg.containerdNamespace, log)
	if err != nil {
		return nil, nil, nil, err
	}
	return images.Containerd(client), client.Healthy, client.Close, nil
}

func kubeConfig(path string) (*rest.Config, error) {
	if path != "" {
		return clientcmd.BuildConfigFromFlags("", path)
	}
	return rest.InClusterConfig()
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}

func env(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if value, err := time.ParseDuration(env(key, "")); err == nil {
		return value
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	if value, err := strconv.ParseBool(env(key, "")); err == nil {
		return value
	}
	return fallback
}
