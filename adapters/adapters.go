package adapters

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

// framedConn serializes one request/response pair and honors context while
// reading fixed-length protocol frames.
type framedConn struct {
	conn net.Conn
	mu   sync.Mutex
}

func (c *framedConn) roundTrip(ctx context.Context, request, response []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetWriteDeadline(deadline)
		_ = c.conn.SetReadDeadline(deadline)
		defer func() {
			_ = c.conn.SetWriteDeadline(time.Time{})
			_ = c.conn.SetReadDeadline(time.Time{})
		}()
	}
	n, err := c.conn.Write(request)
	if err != nil {
		if n > 0 {
			return uncertainWrite(err)
		}
		return err
	}
	if _, err := io.ReadFull(c.conn, response); err != nil {
		// Request bytes were already handed to the transport. The peer may have
		// executed an operation before timeout/reset, so never call this safe.
		return uncertainWrite(err)
	}
	return nil
}

type uncertainWriteError struct{ err error }

func (e *uncertainWriteError) Error() string { return e.err.Error() }
func (e *uncertainWriteError) Unwrap() error { return e.err }

func uncertainWrite(err error) error {
	if err == nil || errors.Is(err, context.Canceled) {
		return err
	}
	return &uncertainWriteError{err: err}
}

func isUncertain(err error) bool {
	var target *uncertainWriteError
	return errors.As(err, &target)
}
