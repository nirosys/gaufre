package gaufre

import (
	"context"
	"time"

	"github.com/nirosys/gaufre/graph"

	"github.com/rs/xid"
)

type ctxKey uint

const (
	ctxKeyNode   ctxKey = 1
	ctxKeyID     ctxKey = 2
	ctxStartTime ctxKey = 3
)

func NewContextFromNode(ctx context.Context, node *graph.Node) context.Context {
	return context.WithValue(ctx, ctxKeyNode, node)
}

func NodeFromContext(ctx context.Context) (*graph.Node, bool) {
	u, ok := ctx.Value(ctxKeyNode).(*graph.Node)
	return u, ok
}

func NewContextWithID(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxKeyID, xid.New().String())
}

func IDFromContext(ctx context.Context) (string, bool) {
	if i := ctx.Value(ctxKeyID); i != nil {
		id, ok := i.(string)
		return id, ok
	} else {
		return "", false
	}
}

func NewContextWithStartTime(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxStartTime, time.Now())
}

func StartTimeFromContext(ctx context.Context) (time.Time, bool) {
	if t := ctx.Value(ctxStartTime); t != nil {
		start, ok := t.(time.Time)
		return start, ok
	} else {
		return time.Time{}, false
	}
}
