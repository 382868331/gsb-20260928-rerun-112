package gateway

import "context"

type Adapter interface {
	Name() string
	Read(ctx context.Context, ref FieldRef, trace *Trace) (Value, error)
	Write(ctx context.Context, ref FieldRef, value Value, trace *Trace) error
	Close() error
}
