package crio

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Subscribe reports changes to the image store and container records. CRI-O
// has no event stream for images, so it watches the containers/storage JSON
// files, which are replaced on every pull, removal, tag, container creation
// and container removal. It sends true after the watch is re-established,
// when changes may have been missed, and coalesces changes the receiver has
// not consumed yet.
func (c *Client) Subscribe(ctx context.Context) <-chan bool {
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
			err := watch(ctx, []string{c.Storage.ImagesDir(), c.Storage.ContainersDir()}, func() {
				backoff = time.Second
				notify(false)
			})
			if ctx.Err() != nil {
				return
			}
			c.log.Warn("image store watch ended", "error", err, "retry", backoff)
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

// watch calls changed when a JSON file in one of dirs is written or replaced,
// until ctx is done or a directory goes away.
func watch(ctx context.Context, dirs []string, changed func()) error {
	fd, err := unix.InotifyInit1(unix.IN_CLOEXEC | unix.IN_NONBLOCK)
	if err != nil {
		return fmt.Errorf("inotify: %w", err)
	}
	// A non-blocking descriptor wrapped in a file uses the runtime poller, so
	// closing it interrupts a pending read.
	file := os.NewFile(uintptr(fd), "inotify")
	defer file.Close()
	stop := context.AfterFunc(ctx, func() { file.Close() })
	defer stop()
	for _, dir := range dirs {
		if _, err := unix.InotifyAddWatch(fd, dir, unix.IN_CLOSE_WRITE|unix.IN_MOVED_TO|unix.IN_DELETE|unix.IN_DELETE_SELF|unix.IN_MOVE_SELF); err != nil {
			return fmt.Errorf("watch %s: %w", dir, err)
		}
	}
	buf := make([]byte, 64*(unix.SizeofInotifyEvent+unix.NAME_MAX+1))
	for {
		n, err := file.Read(buf)
		if err != nil {
			if errors.Is(err, os.ErrClosed) {
				return ctx.Err()
			}
			return err
		}
		relevant := false
		for offset := 0; offset+unix.SizeofInotifyEvent <= n; {
			event := (*unix.InotifyEvent)(unsafe.Pointer(&buf[offset]))
			nameStart := offset + unix.SizeofInotifyEvent
			name := strings.TrimRight(string(buf[nameStart:nameStart+int(event.Len)]), "\x00")
			offset = nameStart + int(event.Len)
			if event.Mask&(unix.IN_DELETE_SELF|unix.IN_MOVE_SELF|unix.IN_IGNORED|unix.IN_Q_OVERFLOW) != 0 {
				return fmt.Errorf("watched directory replaced or events overflowed")
			}
			if strings.HasSuffix(name, ".json") {
				relevant = true
			}
		}
		if relevant {
			changed()
		}
	}
}
