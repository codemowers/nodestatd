// Package crio connects nodestatd modules to CRI-O: its CRI socket for the
// images and containers kubelet sees, and its containers/storage image store
// for image content.
package crio

import (
	"context"
	"fmt"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// Client is a CRI client for CRI-O together with a reader for its image
// store.
type Client struct {
	conn    *grpc.ClientConn
	runtime runtimeapi.RuntimeServiceClient
	images  runtimeapi.ImageServiceClient
	Storage *Storage
	log     *slog.Logger
}

// Connect returns a client for the CRI socket at address that reads image
// content from storage. Connections are established lazily and
// re-established after CRI-O restarts.
func Connect(address string, storage *Storage, log *slog.Logger) (*Client, error) {
	conn, err := grpc.NewClient("unix://"+address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("cri-o: %w", err)
	}
	return &Client{
		conn:    conn,
		runtime: runtimeapi.NewRuntimeServiceClient(conn),
		images:  runtimeapi.NewImageServiceClient(conn),
		Storage: storage,
		log:     log,
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// Healthy reports whether CRI-O is serving the CRI.
func (c *Client) Healthy(ctx context.Context) error {
	if _, err := c.runtime.Version(ctx, &runtimeapi.VersionRequest{}); err != nil {
		return fmt.Errorf("cri-o: %w", err)
	}
	return nil
}

// ListImages returns the images CRI-O reports to kubelet.
func (c *Client) ListImages(ctx context.Context) ([]*runtimeapi.Image, error) {
	resp, err := c.images.ListImages(ctx, &runtimeapi.ListImagesRequest{})
	if err != nil {
		return nil, fmt.Errorf("list images: %w", err)
	}
	return resp.Images, nil
}

// ContainerImages returns the image IDs and references containers were
// created from. Containers count whether running or exited, as for kubelet
// image garbage collection.
func (c *Client) ContainerImages(ctx context.Context) (map[string]bool, error) {
	resp, err := c.runtime.ListContainers(ctx, &runtimeapi.ListContainersRequest{})
	if err != nil {
		return nil, fmt.Errorf("list containers: %w", err)
	}
	images := map[string]bool{}
	for _, container := range resp.Containers {
		for _, ref := range []string{container.ImageId, container.ImageRef, container.GetImage().GetImage()} {
			if ref != "" {
				images[ref] = true
			}
		}
	}
	return images, nil
}
