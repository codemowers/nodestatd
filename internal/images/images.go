// Package images is the nodestatd module that records the node's containerd
// images as ContainerImage objects, owning ImageRepository and
// SourceRepository objects.
package images

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/codemowers/nodestatd/internal/containerd"
	"github.com/codemowers/nodestatd/internal/images/inventory"
	"github.com/codemowers/nodestatd/internal/images/publisher"
	"github.com/containerd/platforms"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/metadata"
)

const retryInitial = 10 * time.Second

type Config struct {
	// ResyncInterval is the full reconciliation interval.
	ResyncInterval time.Duration
	// Debounce delays syncs after containerd image and container events.
	Debounce time.Duration
	// RecordEnv records image config environment variables.
	RecordEnv bool
}

type Module struct {
	cfg       Config
	client    *containerd.Client
	publisher *publisher.Publisher
	log       *slog.Logger
	ready     atomic.Bool
}

func New(cfg Config, client *containerd.Client, dynamicClient dynamic.Interface, metadataClient metadata.Interface, node string, log *slog.Logger) *Module {
	log = log.With("module", "images")
	return &Module{
		cfg:       cfg,
		client:    client,
		publisher: publisher.New(dynamicClient, nodeLister(metadataClient), node, log),
		log:       log,
	}
}

// Ready reports whether a full reconciliation has succeeded.
func (m *Module) Ready(context.Context) error {
	if !m.ready.Load() {
		return fmt.Errorf("initial image reconciliation pending")
	}
	return nil
}

// Run reconciles until ctx is done: incrementally after containerd events and
// fully on start, after resubscribing and every resync interval.
func (m *Module) Run(ctx context.Context) error {
	events := m.client.Subscribe(ctx, "^/(images|containers)/")
	resync := time.NewTicker(m.cfg.ResyncInterval)
	defer resync.Stop()
	var debounce, retry <-chan time.Time
	retryDelay := retryInitial
	// Failed full reconciliations, such as before the CRD is established,
	// retry with backoff instead of waiting for the resync interval.
	fullReconcile := func() {
		if m.reconcile(ctx, true) {
			retry, retryDelay = nil, retryInitial
			return
		}
		retry = time.After(retryDelay)
		retryDelay = min(retryDelay*2, m.cfg.ResyncInterval)
	}
	fullReconcile()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case resubscribed := <-events:
			// Events may have been missed while the subscription was down.
			if resubscribed {
				fullReconcile()
				continue
			}
			if debounce == nil {
				debounce = time.After(m.cfg.Debounce)
			}
		case <-debounce:
			debounce = nil
			m.reconcile(ctx, false)
		case <-retry:
			fullReconcile()
		case <-resync.C:
			fullReconcile()
		}
	}
}

func (m *Module) reconcile(ctx context.Context, full bool) bool {
	local, err := m.collect(ctx)
	if err == nil {
		if full {
			err = m.publisher.Resync(ctx, local)
		} else {
			err = m.publisher.Sync(ctx, local)
		}
	}
	if err != nil {
		if ctx.Err() == nil {
			m.log.Error("reconcile failed", "full", full, "error", err)
		}
		return false
	}
	if full {
		m.ready.Store(true)
	}
	m.log.Info("reconciled", "full", full, "images", len(local))
	return true
}

func (m *Module) collect(ctx context.Context) (map[string]*inventory.Image, error) {
	records, err := m.client.ImageService().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list images: %w", err)
	}
	inUse, err := m.client.ContainerImages(ctx)
	if err != nil {
		return nil, err
	}
	local, err := inventory.Collect(ctx, records, m.client.ContentStore(), inventory.Options{
		Platform:  platforms.Default(),
		RecordEnv: m.cfg.RecordEnv,
		InUse:     inUse,
	})
	if err != nil {
		// Unresolvable records are skipped; the rest are still published.
		m.log.Warn("skipped image records", "error", err)
	}
	return local, nil
}

func nodeLister(client metadata.Interface) publisher.NodeLister {
	nodes := client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "nodes"})
	return func(ctx context.Context) (map[string]types.UID, error) {
		list, err := nodes.List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		uids := make(map[string]types.UID, len(list.Items))
		for _, node := range list.Items {
			uids[node.Name] = node.UID
		}
		return uids, nil
	}
}
