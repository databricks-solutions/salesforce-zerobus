package zerobus

import (
	"context"
	"errors"
	"sync"

	zb "github.com/databricks/zerobus-sdk/purego/zerobus"
)

// SDKConfig configures the real Zerobus factory.
type SDKConfig struct {
	ZerobusEndpoint string
	UCEndpoint      string
	ClientID        string
	ClientSecret    string
	AppName         string
	// StreamsPerSDK caps streams multiplexed on one SDK (one gRPC
	// connection). HTTP/2 servers limit concurrent streams per connection,
	// and gRPC queues streams past the limit silently, so stay conservative.
	StreamsPerSDK int
	// StreamOptions apply to every stream (buffering, recovery, timeouts).
	StreamOptions []zb.StreamOption
}

// SDKFactory opens streams through a pool of SDK instances. Streams on one
// SDK share its gRPC connection; all streams share one token provider.
type SDKFactory struct {
	cfg        SDKConfig
	descriptor []byte
	tokens     *tokenProvider

	mu     sync.Mutex
	sdks   []*sdkEntry
	closed bool
}

type sdkEntry struct {
	sdk     *zb.SDK
	streams int
}

// NewSDKFactory validates the configuration. SDKs are created lazily.
func NewSDKFactory(cfg SDKConfig) (*SDKFactory, error) {
	if cfg.StreamsPerSDK <= 0 {
		cfg.StreamsPerSDK = 50
	}
	desc, err := Descriptor()
	if err != nil {
		return nil, err
	}
	tokens, err := newTokenProvider(cfg.ZerobusEndpoint, cfg.UCEndpoint, cfg.ClientID, cfg.ClientSecret, nil)
	if err != nil {
		return nil, err
	}
	return &SDKFactory{cfg: cfg, descriptor: desc, tokens: tokens}, nil
}

func (f *SDKFactory) acquire() (*sdkEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, errors.New("zerobus factory closed")
	}
	for _, e := range f.sdks {
		if e.streams < f.cfg.StreamsPerSDK {
			e.streams++
			return e, nil
		}
	}
	var opts []zb.Option
	if f.cfg.AppName != "" {
		opts = append(opts, zb.WithApplicationName(f.cfg.AppName))
	}
	sdk, err := zb.New(f.cfg.ZerobusEndpoint, f.cfg.UCEndpoint, opts...)
	if err != nil {
		return nil, err
	}
	e := &sdkEntry{sdk: sdk, streams: 1}
	f.sdks = append(f.sdks, e)
	return e, nil
}

func (f *SDKFactory) release(e *sdkEntry) {
	f.mu.Lock()
	e.streams--
	f.mu.Unlock()
}

// Open creates a proto stream for table and waits until it is open.
func (f *SDKFactory) Open(ctx context.Context, table string, cb zb.AckCallback) (Stream, error) {
	e, err := f.acquire()
	if err != nil {
		return nil, err
	}
	opts := append([]zb.StreamOption{}, f.cfg.StreamOptions...)
	opts = append(opts, zb.WithProto(f.descriptor), zb.WithWaitForReady(), zb.WithAckCallback(cb))
	st, err := e.sdk.CreateStreamWithProvider(ctx, table, f.tokens, opts...)
	if err != nil {
		f.release(e)
		return nil, err
	}
	return &sdkStream{Stream: st, release: func() { f.release(e) }}, nil
}

// Close closes every SDK. Streams must already be closed (SDK.Close does
// not flush).
func (f *SDKFactory) Close() error {
	f.mu.Lock()
	f.closed = true
	sdks := f.sdks
	f.sdks = nil
	f.mu.Unlock()
	var errs []error
	for _, e := range sdks {
		errs = append(errs, e.sdk.Close())
	}
	return errors.Join(errs...)
}

type sdkStream struct {
	*zb.Stream
	once    sync.Once
	release func()
}

func (s *sdkStream) Close() error {
	err := s.Stream.Close()
	s.once.Do(s.release)
	return err
}
