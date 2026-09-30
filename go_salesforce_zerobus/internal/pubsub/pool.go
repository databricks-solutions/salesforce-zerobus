package pubsub

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/proto/pubsubpb"
)

// ConnPool shares gRPC connections to the Pub/Sub API across tenants. The
// endpoint is the same for every org (auth travels in per-RPC metadata), so
// subscriptions are packed onto connections up to perConn streams each.
// Staying under the server's HTTP/2 concurrent-stream limit matters: gRPC
// queues streams past it silently.
type ConnPool struct {
	addr    string
	perConn int
	opts    []grpc.DialOption

	mu     sync.Mutex
	conns  []*poolConn
	closed bool
}

type poolConn struct {
	cc     *grpc.ClientConn
	client pubsubpb.PubSubClient
	users  int
}

// NewConnPool creates a pool for addr (host:port). insecureTransport is for
// tests against a local fake only.
func NewConnPool(addr string, perConn int, insecureTransport bool) *ConnPool {
	if perConn <= 0 {
		perConn = 100
	}
	creds := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	if insecureTransport {
		creds = insecure.NewCredentials()
	}
	return &ConnPool{addr: addr, perConn: perConn, opts: []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 60 * time.Second, Timeout: 10 * time.Second, PermitWithoutStream: true}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64 << 20)),
		grpc.WithUnaryInterceptor(traceUnary),
		grpc.WithStreamInterceptor(traceStream),
	}}
}

// Acquire returns a client on the least-loaded connection with room, dialing
// a new connection if all are full. Call release when the stream ends.
func (p *ConnPool) Acquire() (pubsubpb.PubSubClient, func(), error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, nil, errors.New("pubsub connection pool closed")
	}
	var best *poolConn
	for _, c := range p.conns {
		if c.users < p.perConn && (best == nil || c.users < best.users) {
			best = c
		}
	}
	if best == nil {
		cc, err := grpc.NewClient(p.addr, p.opts...)
		if err != nil {
			return nil, nil, err
		}
		best = &poolConn{cc: cc, client: pubsubpb.NewPubSubClient(cc)}
		p.conns = append(p.conns, best)
	}
	best.users++
	var once sync.Once
	return best.client, func() {
		once.Do(func() {
			p.mu.Lock()
			best.users--
			p.mu.Unlock()
		})
	}, nil
}

// Stats returns the number of connections and active streams.
func (p *ConnPool) Stats() (conns, streams int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		streams += c.users
	}
	return len(p.conns), streams
}

// Close closes every connection.
func (p *ConnPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	var errs []error
	for _, c := range p.conns {
		errs = append(errs, c.cc.Close())
	}
	p.conns = nil
	return errors.Join(errs...)
}

func traceUnary(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	return invoker(metadata.AppendToOutgoingContext(ctx, "x-client-trace-id", uuid.NewString()), method, req, reply, cc, opts...)
}

func traceStream(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return streamer(metadata.AppendToOutgoingContext(ctx, "x-client-trace-id", uuid.NewString()), desc, cc, method, opts...)
}
