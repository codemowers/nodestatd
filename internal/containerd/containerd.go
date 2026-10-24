// Package containerd shares one containerd connection between nodestatd
// modules.
package containerd

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
)

// Client is a containerd client bound to the namespace holding Kubernetes
// containers and images.
type Client struct {
	*containerd.Client
	Namespace string
	log       *slog.Logger
}

func Connect(address, namespace string, log *slog.Logger) (*Client, error) {
	client, err := containerd.New(address, containerd.WithDefaultNamespace(namespace))
	if err != nil {
		return nil, fmt.Errorf("containerd: %w", err)
	}
	return &Client{Client: client, Namespace: namespace, log: log}, nil
}

// Healthy reports whether containerd is serving.
func (c *Client) Healthy(ctx context.Context) error {
	serving, err := c.IsServing(ctx)
	if err != nil {
		return err
	}
	if !serving {
		return fmt.Errorf("containerd is not serving")
	}
	return nil
}

// Subscribe forwards events in the client's namespace whose topic matches
// topicPattern, a regular expression. It sends true after the subscription is
// re-established, when events may have been missed, and coalesces events the
// receiver has not consumed yet.
func (c *Client) Subscribe(ctx context.Context, topicPattern string) <-chan bool {
	out := make(chan bool, 1)
	notify := func(value bool) {
		select {
		case out <- value:
		default:
		}
	}
	go func() {
		backoff := time.Second
		for first := true; ctx.Err() == nil; first = false {
			if !first {
				notify(true)
			}
			envelopes, errs := c.EventService().Subscribe(ctx,
				fmt.Sprintf(`topic~=%q`, topicPattern), fmt.Sprintf(`namespace==%q`, c.Namespace))
		receive:
			for {
				select {
				case <-ctx.Done():
					return
				case <-envelopes:
					backoff = time.Second
					notify(false)
				case err := <-errs:
					if ctx.Err() == nil {
						c.log.Warn("containerd event subscription ended", "error", err, "retry", backoff)
					}
					break receive
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, time.Minute)
		}
	}()
	return out
}

// ContainerImages returns the image record names containers reference.
// Containers count whether running or exited, as for kubelet image garbage
// collection.
func (c *Client) ContainerImages(ctx context.Context) (map[string]bool, error) {
	containers, err := c.ContainerService().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list containers: %w", err)
	}
	images := make(map[string]bool, len(containers))
	for _, container := range containers {
		images[container.Image] = true
	}
	return images, nil
}
