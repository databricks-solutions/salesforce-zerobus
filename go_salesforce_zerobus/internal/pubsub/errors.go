package pubsub

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/sfauth"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/sink"
)

// Class groups subscription errors by how the runner should react.
type Class int

const (
	ClassTransient       Class = iota // network/server hiccup: resume from last submitted, short backoff
	ClassAuth                         // session rejected: refresh token, retry quickly
	ClassPermanentAuth                // credentials rejected: needs a config/secret fix
	ClassReplayExpired                // stored replay ID aged out: fall back to a preset
	ClassConfig                       // topic/permission/org problem: needs a config fix
	ClassQuota                        // RESOURCE_EXHAUSTED: long backoff
	ClassSinkReset                    // Zerobus lost in-flight rows: resume from acked
	ClassSinkUnavailable              // Zerobus stream down: back off until it recovers
	ClassCanceled                     // shutdown or stop
)

func (c Class) String() string {
	return [...]string{"transient", "auth", "permanent_auth", "replay_expired", "config", "quota", "sink_reset", "sink_unavailable", "canceled"}[c]
}

// Error is a classified subscription error.
type Error struct {
	Class Class
	Err   error
}

func (e *Error) Error() string { return e.Class.String() + ": " + e.Err.Error() }
func (e *Error) Unwrap() error { return e.Err }

// errSinkReset is returned when the sink reports lost in-flight rows.
var errSinkReset = errors.New("zerobus stream failed with rows in flight")

// errOrgMismatch is returned when the authenticated org differs from the
// configured org_id.
var errOrgMismatch = errors.New("authenticated org does not match configured org_id")

// Classify maps err to a Class.
func Classify(err error) Class {
	var e *Error
	if errors.As(err, &e) {
		return e.Class
	}
	switch {
	case errors.Is(err, context.Canceled):
		return ClassCanceled
	case errors.Is(err, errSinkReset):
		return ClassSinkReset
	case errors.Is(err, sink.ErrUnavailable):
		return ClassSinkUnavailable
	case errors.Is(err, sink.ErrClosed):
		return ClassCanceled
	case errors.Is(err, errOrgMismatch):
		return ClassConfig
	}
	var ae *sfauth.Error
	if errors.As(err, &ae) {
		if ae.Permanent {
			return ClassPermanentAuth
		}
		return ClassTransient
	}
	st, ok := status.FromError(err)
	if !ok {
		return ClassTransient
	}
	switch st.Code() {
	case codes.Unauthenticated:
		return ClassAuth
	case codes.InvalidArgument:
		if mentionsReplay(st) {
			return ClassReplayExpired
		}
		return ClassConfig
	case codes.PermissionDenied, codes.NotFound, codes.FailedPrecondition, codes.Unimplemented:
		return ClassConfig
	case codes.ResourceExhausted:
		return ClassQuota
	case codes.Canceled:
		return ClassCanceled
	default: // Unavailable, Internal, Aborted, DeadlineExceeded, Unknown, DataLoss
		return ClassTransient
	}
}

func mentionsReplay(st *status.Status) bool {
	if strings.Contains(strings.ToLower(st.Message()), "replay") {
		return true
	}
	for _, d := range st.Details() {
		if strings.Contains(strings.ToLower(fmt.Sprint(d)), "replay") {
			return true
		}
	}
	return false
}

func classify(err error) error {
	if err == nil {
		return nil
	}
	var e *Error
	if errors.As(err, &e) {
		return err
	}
	return &Error{Class: Classify(err), Err: err}
}
