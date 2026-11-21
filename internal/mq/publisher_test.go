package mq

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"wallpaper-feed/internal/config"
)

// fakeAMQPBehavior 控制测试 AMQP 0-9-1 协议桩在各阶段的行为，用于确定性模拟网络/Broker 挂起与异常。
type fakeAMQPBehavior struct {
	mu                     sync.Mutex
	hangExchangeDeclare    bool
	hangChannelOpenAfterN  int // >0 时在第 N 次 Channel.Open 之后（含第 N 次）不回 Channel.OpenOk
	hangPublishConfirm     bool
	hangChannelClose       bool
	hangConnectionClose    bool
	nackPublish            bool
	channelNotFoundOnKey   string // RoutingKey 命中时回 Channel.Close(404)
	exchangeDeclareEntered chan struct{}
	channelOpenCount       int
	publishedCount         atomic.Int32
}

func (b *fakeAMQPBehavior) snapshot() (hangEx, hangConfirm, hangChClose, hangConnClose, nack bool, notFoundKey string, hangChAfter int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.hangExchangeDeclare, b.hangPublishConfirm, b.hangChannelClose, b.hangConnectionClose, b.nackPublish, b.channelNotFoundOnKey, b.hangChannelOpenAfterN
}

type fakeAMQPBroker struct {
	ln       net.Listener
	behavior *fakeAMQPBehavior
	wg       sync.WaitGroup
	stopCh   chan struct{}
	connsMu  sync.Mutex
	conns    []net.Conn
}

func startFakeAMQPBroker(t *testing.T, behavior *fakeAMQPBehavior) *fakeAMQPBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen fake amqp broker: %v", err)
	}
	if behavior == nil {
		behavior = &fakeAMQPBehavior{}
	}
	b := &fakeAMQPBroker{
		ln:       ln,
		behavior: behavior,
		stopCh:   make(chan struct{}),
	}
	b.wg.Add(1)
	go b.acceptLoop()
	t.Cleanup(func() { b.Close() })
	return b
}

func (b *fakeAMQPBroker) URL() string {
	return "amqp://guest:guest@" + b.ln.Addr().String() + "/"
}

func (b *fakeAMQPBroker) Close() {
	select {
	case <-b.stopCh:
		return
	default:
		close(b.stopCh)
	}
	_ = b.ln.Close()
	b.connsMu.Lock()
	for _, c := range b.conns {
		_ = c.Close()
	}
	b.connsMu.Unlock()
	b.wg.Wait()
}

func (b *fakeAMQPBroker) acceptLoop() {
	defer b.wg.Done()
	for {
		c, err := b.ln.Accept()
		if err != nil {
			return
		}
		b.connsMu.Lock()
		b.conns = append(b.conns, c)
		b.connsMu.Unlock()
		b.wg.Add(1)
		go func(conn net.Conn) {
			defer b.wg.Done()
			defer func() { _ = conn.Close() }()
			b.serveConn(conn)
		}(c)
	}
}

func (b *fakeAMQPBroker) serveConn(conn net.Conn) {
	r := bufio.NewReader(conn)
	hdr := make([]byte, 8)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return
	}
	// 1. 发送 Connection.Start (class=10, method=10)
	if err := writeAMQPMethodFrame(conn, 0, 10, 10, encodeConnectionStart()); err != nil {
		return
	}

	confirmSeq := make(map[uint16]uint64)
	pendingPubCh := uint16(0)
	pendingPubKey := ""
	pendingBodyRemain := uint64(0)
	awaitingHeader := false

	for {
		frameType, channelID, payload, err := readAMQPFrame(r)
		if err != nil {
			return
		}
		switch frameType {
		case 1: // Method
			if len(payload) < 4 {
				return
			}
			classID := binary.BigEndian.Uint16(payload[0:2])
			methodID := binary.BigEndian.Uint16(payload[2:4])
			args := payload[4:]

			hangEx, _, hangChClose, hangConnClose, _, _, hangChAfter := b.behavior.snapshot()

			switch {
			case classID == 10 && methodID == 11: // Connection.StartOk -> Connection.Tune
				if err := writeAMQPMethodFrame(conn, 0, 10, 30, encodeConnectionTune()); err != nil {
					return
				}
			case classID == 10 && methodID == 31: // Connection.TuneOk
			case classID == 10 && methodID == 40: // Connection.Open -> Connection.OpenOk
				if err := writeAMQPMethodFrame(conn, 0, 10, 41, []byte{0}); err != nil {
					return
				}
			case classID == 10 && methodID == 50: // Connection.Close
				if hangConnClose {
					<-b.stopCh
					return
				}
				_ = writeAMQPMethodFrame(conn, 0, 10, 51, nil)
				return
			case classID == 20 && methodID == 10: // Channel.Open
				b.behavior.mu.Lock()
				b.behavior.channelOpenCount++
				cnt := b.behavior.channelOpenCount
				b.behavior.mu.Unlock()
				if hangChAfter > 0 && cnt >= hangChAfter {
					<-b.stopCh
					return
				}
				if err := writeAMQPMethodFrame(conn, channelID, 20, 11, []byte{0, 0, 0, 0}); err != nil {
					return
				}
			case classID == 20 && methodID == 40: // Channel.Close
				if hangChClose {
					<-b.stopCh
					return
				}
				if err := writeAMQPMethodFrame(conn, channelID, 20, 41, nil); err != nil {
					return
				}
			case classID == 40 && methodID == 10: // Exchange.Declare
				if hangEx {
					b.behavior.mu.Lock()
					ch := b.behavior.exchangeDeclareEntered
					b.behavior.exchangeDeclareEntered = nil
					b.behavior.mu.Unlock()
					if ch != nil {
						close(ch)
					}
					<-b.stopCh
					return
				}
				if err := writeAMQPMethodFrame(conn, channelID, 40, 11, nil); err != nil {
					return
				}
			case classID == 50 && methodID == 10: // Queue.Declare
				if err := writeAMQPMethodFrame(conn, channelID, 50, 11, encodeQueueDeclareOk("")); err != nil {
					return
				}
			case classID == 50 && methodID == 20: // Queue.Bind
				if err := writeAMQPMethodFrame(conn, channelID, 50, 21, nil); err != nil {
					return
				}
			case classID == 85 && methodID == 10: // Confirm.Select
				confirmSeq[channelID] = 0
				if err := writeAMQPMethodFrame(conn, channelID, 85, 11, nil); err != nil {
					return
				}
			case classID == 60 && methodID == 40: // Basic.Publish
				pendingPubCh = channelID
				pendingPubKey = parsePublishRoutingKey(args)
				awaitingHeader = true
				pendingBodyRemain = 0
			}

		case 2: // Content Header
			if !awaitingHeader || len(payload) < 12 {
				continue
			}
			pendingBodyRemain = binary.BigEndian.Uint64(payload[4:12])
			awaitingHeader = false
			if pendingBodyRemain == 0 {
				if !b.finishPublish(conn, pendingPubCh, pendingPubKey, confirmSeq) {
					return
				}
			}

		case 3: // Content Body
			if uint64(len(payload)) >= pendingBodyRemain {
				pendingBodyRemain = 0
				if !b.finishPublish(conn, pendingPubCh, pendingPubKey, confirmSeq) {
					return
				}
			} else {
				pendingBodyRemain -= uint64(len(payload))
			}

		case 8: // Heartbeat
			_ = writeAMQPFrame(conn, 8, 0, nil)
		}
	}
}

func (b *fakeAMQPBroker) finishPublish(conn net.Conn, chID uint16, routingKey string, confirmSeq map[uint16]uint64) bool {
	_, hangConfirm, _, _, nack, notFoundKey, _ := b.behavior.snapshot()
	if notFoundKey != "" && routingKey == notFoundKey {
		// 模拟 Channel 级 404 NOT_FOUND 错误（不关闭 TCP 连接，仅关闭该 Channel）。
		if err := writeAMQPMethodFrame(conn, chID, 20, 40, encodeChannelClose(amqp.NotFound, "no exchange", 60, 40)); err != nil {
			return false
		}
		return true
	}
	b.behavior.publishedCount.Add(1)
	if hangConfirm {
		return true
	}
	confirmSeq[chID]++
	tag := confirmSeq[chID]
	if nack {
		if err := writeAMQPMethodFrame(conn, chID, 60, 120, encodeBasicAckOrNack(tag)); err != nil {
			return false
		}
		return true
	}
	if err := writeAMQPMethodFrame(conn, chID, 60, 80, encodeBasicAckOrNack(tag)); err != nil {
		return false
	}
	return true
}

func readAMQPFrame(r *bufio.Reader) (byte, uint16, []byte, error) {
	var hdr [7]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, 0, nil, err
	}
	frameType := hdr[0]
	channelID := binary.BigEndian.Uint16(hdr[1:3])
	size := binary.BigEndian.Uint32(hdr[3:7])
	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, 0, nil, err
	}
	end, err := r.ReadByte()
	if err != nil {
		return 0, 0, nil, err
	}
	if end != 0xCE {
		return 0, 0, nil, fmt.Errorf("invalid frame end byte: %x", end)
	}
	return frameType, channelID, payload, nil
}

func writeAMQPMethodFrame(w io.Writer, channelID, classID, methodID uint16, args []byte) error {
	payload := make([]byte, 4+len(args))
	binary.BigEndian.PutUint16(payload[0:2], classID)
	binary.BigEndian.PutUint16(payload[2:4], methodID)
	copy(payload[4:], args)
	return writeAMQPFrame(w, 1, channelID, payload)
}

func writeAMQPFrame(w io.Writer, frameType byte, channelID uint16, payload []byte) error {
	buf := make([]byte, 7+len(payload)+1)
	buf[0] = frameType
	binary.BigEndian.PutUint16(buf[1:3], channelID)
	binary.BigEndian.PutUint32(buf[3:7], uint32(len(payload)))
	copy(buf[7:], payload)
	buf[len(buf)-1] = 0xCE
	_, err := w.Write(buf)
	return err
}

func encodeConnectionStart() []byte {
	var out []byte
	out = append(out, 0, 9)               // AMQP 0-9
	out = append(out, 0, 0, 0, 0)         // empty server-properties table
	out = appendLongStr(out, "PLAIN")     // mechanisms
	out = appendLongStr(out, "en_US")     // locales
	return out
}

func encodeConnectionTune() []byte {
	var out [8]byte
	binary.BigEndian.PutUint16(out[0:2], 2047)
	binary.BigEndian.PutUint32(out[2:6], 131072)
	binary.BigEndian.PutUint16(out[6:8], 0) // heartbeat=0，避免测试中额外心跳干扰帧序
	return out[:]
}

func encodeQueueDeclareOk(name string) []byte {
	out := make([]byte, 1+len(name)+8)
	out[0] = byte(len(name))
	copy(out[1:], name)
	return out
}

func encodeBasicAckOrNack(tag uint64) []byte {
	var out [9]byte
	binary.BigEndian.PutUint64(out[0:8], tag)
	out[8] = 0
	return out[:]
}

func encodeChannelClose(code uint16, text string, classID, methodID uint16) []byte {
	out := make([]byte, 2+1+len(text)+4)
	binary.BigEndian.PutUint16(out[0:2], code)
	out[2] = byte(len(text))
	copy(out[3:], text)
	pos := 3 + len(text)
	binary.BigEndian.PutUint16(out[pos:pos+2], classID)
	binary.BigEndian.PutUint16(out[pos+2:pos+4], methodID)
	return out
}

func appendLongStr(dst []byte, s string) []byte {
	var lb [4]byte
	binary.BigEndian.PutUint32(lb[:], uint32(len(s)))
	dst = append(dst, lb[:]...)
	return append(dst, s...)
}

func parsePublishRoutingKey(args []byte) string {
	// Basic.Publish args: reserved1(2B) + exchange(shortstr) + routingKey(shortstr) + bits(1B)
	if len(args) < 4 {
		return ""
	}
	exLen := int(args[2])
	rkPos := 3 + exLen
	if rkPos >= len(args) {
		return ""
	}
	rkLen := int(args[rkPos])
	if rkPos+1+rkLen > len(args) {
		return ""
	}
	return string(args[rkPos+1 : rkPos+1+rkLen])
}

// blockingWriteConn 包装真实 net.Conn，在开启 blockWrites 后让 Write 真实阻塞，
// 仅在底层连接被 Close() 或 SetDeadline/SetWriteDeadline 到期时解除阻塞（完整还原阻塞 TCP socket 行为）。
type blockingWriteConn struct {
	net.Conn
	blockWrites atomic.Bool

	writeEnteredOnce sync.Once
	writeEnteredCh   chan struct{}
	writeExitedOnce  sync.Once
	writeExitedCh    chan struct{}

	mu            sync.Mutex
	closed        bool
	closedCh      chan struct{}
	writeDeadline time.Time
	deadlineCh    chan struct{}
}

func newBlockingWriteConn(c net.Conn) *blockingWriteConn {
	return &blockingWriteConn{
		Conn:           c,
		writeEnteredCh: make(chan struct{}),
		writeExitedCh:  make(chan struct{}),
		closedCh:       make(chan struct{}),
		deadlineCh:     make(chan struct{}, 1),
	}
}

func (b *blockingWriteConn) Write(p []byte) (int, error) {
	if !b.blockWrites.Load() {
		return b.Conn.Write(p)
	}
	b.writeEnteredOnce.Do(func() { close(b.writeEnteredCh) })
	defer b.writeExitedOnce.Do(func() { close(b.writeExitedCh) })

	for {
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return 0, net.ErrClosed
		}
		dl := b.writeDeadline
		b.mu.Unlock()

		var timer *time.Timer
		var timerC <-chan time.Time
		if !dl.IsZero() {
			rem := time.Until(dl)
			if rem <= 0 {
				return 0, os.ErrDeadlineExceeded
			}
			timer = time.NewTimer(rem)
			timerC = timer.C
		}

		select {
		case <-b.closedCh:
			if timer != nil {
				timer.Stop()
			}
			return 0, net.ErrClosed
		case <-timerC:
			return 0, os.ErrDeadlineExceeded
		case <-b.deadlineCh:
			if timer != nil {
				timer.Stop()
			}
		}
	}
}

func (b *blockingWriteConn) SetDeadline(t time.Time) error {
	b.updateWriteDeadline(t)
	return b.Conn.SetDeadline(t)
}

func (b *blockingWriteConn) SetWriteDeadline(t time.Time) error {
	b.updateWriteDeadline(t)
	return b.Conn.SetWriteDeadline(t)
}

func (b *blockingWriteConn) updateWriteDeadline(t time.Time) {
	b.mu.Lock()
	b.writeDeadline = t
	b.mu.Unlock()
	select {
	case b.deadlineCh <- struct{}{}:
	default:
	}
}

func (b *blockingWriteConn) Close() error {
	b.mu.Lock()
	if !b.closed {
		b.closed = true
		close(b.closedCh)
	}
	b.mu.Unlock()
	return b.Conn.Close()
}

func testMQConfig(url string) config.RabbitMQConfig {
	return config.RabbitMQConfig{
		URL:               url,
		Exchange:          "wallpaper.events",
		DLXExchange:       "wallpaper.dlx",
		FanoutQueue:       "feed.following.fanout",
		RetryQueue:        "feed.following.fanout.retry",
		DLQQueue:          "feed.following.fanout.dlq",
		RetryDelayMs:      5000,
		MaxConsumeRetries: 5,
	}
}

// TestConfirmPublisher_PublishWriteBlockTimeoutUnblocksAndReconnects 验证：
// 底层 TCP Write 阻塞时，Publish(ctx) 在期限内返回连接超时错误、真正打断底层 Write（无后台阻塞遗留），
// 并触发自动重连使后续消息在新连接上成功发布。
func TestConfirmPublisher_PublishWriteBlockTimeoutUnblocksAndReconnects(t *testing.T) {
	broker := startFakeAMQPBroker(t, nil)

	var connMu sync.Mutex
	var firstConn *blockingWriteConn
	var dialCount int

	client, err := newClientWithOptions(
		testMQConfig(broker.URL()),
		func(c *Client) {
			c.dialTimeout = time.Second
			c.setupTimeout = time.Second
			c.writeTimeout = 5 * time.Second
			c.closeTimeout = 300 * time.Millisecond
			c.heartbeat = 0
			c.initialBackoff = 30 * time.Millisecond
			c.maxBackoff = 100 * time.Millisecond
			c.dialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
				nc, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				connMu.Lock()
				dialCount++
				idx := dialCount
				connMu.Unlock()
				if idx == 1 {
					bc := newBlockingWriteConn(nc)
					connMu.Lock()
					firstConn = bc
					connMu.Unlock()
					return bc, nil
				}
				return nc, nil
			}
		},
	)
	if err != nil {
		t.Fatalf("newClientWithOptions: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	pub := NewConfirmPublisher(client)
	t.Cleanup(func() { _ = pub.Close() })

	// 1. 先发送一条正常消息，验证通道已建立且 Broker 正常确认。
	if err := pub.Publish(context.Background(), Message{
		Exchange: "wallpaper.events", RoutingKey: "post.created", MessageID: "msg-ok-1", Body: []byte(`{"id":1}`),
	}); err != nil {
		t.Fatalf("initial healthy publish failed: %v", err)
	}

	// 2. 开启底层 Write 阻塞，触发 150ms 超时发布。
	connMu.Lock()
	bc := firstConn
	connMu.Unlock()
	if bc == nil {
		t.Fatal("expected firstConn captured")
	}
	bc.blockWrites.Store(true)

	pubCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	pubErr := pub.Publish(pubCtx, Message{
		Exchange: "wallpaper.events", RoutingKey: "post.created", MessageID: "msg-blocked", Body: []byte(`{"id":2}`),
	})
	elapsed := time.Since(start)

	// 确认测试确实走进了底层 Write 阻塞，且底层 Write 已真正退出（绝非仅外层返回超时留僵尸写）。
	select {
	case <-bc.writeEnteredCh:
	default:
		t.Fatal("expected Publish to enter underlying blocked Write")
	}
	select {
	case <-bc.writeExitedCh:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("underlying blocked Write did not exit after publish timeout")
	}

	if pubErr == nil {
		t.Fatal("expected publish error on blocked write timeout")
	}
	if !IsConnectionError(pubErr) || !errors.Is(pubErr, context.DeadlineExceeded) {
		t.Fatalf("expected connection + deadline error, got: %v", pubErr)
	}
	if elapsed > 800*time.Millisecond {
		t.Fatalf("publish timeout took too long: %v", elapsed)
	}

	// 3. 等待自动重连完成后，再次发布应在新连接上成功。
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	if _, err := client.WaitConnection(waitCtx); err != nil {
		t.Fatalf("client failed to reconnect after blocked publish: %v", err)
	}
	if err := pub.Publish(waitCtx, Message{
		Exchange: "wallpaper.events", RoutingKey: "post.created", MessageID: "msg-recovered", Body: []byte(`{"id":3}`),
	}); err != nil {
		t.Fatalf("publish after reconnect failed: %v", err)
	}
	if got := broker.behavior.publishedCount.Load(); got != 2 {
		t.Fatalf("expected 2 published messages (initial + recovered), got %d", got)
	}
}

// TestConfirmPublisher_CloseUnblocksInFlightBlockedPublish 验证：
// 当 Publish 正持锁阻塞在底层网络 Write 时，调用 Close() 立即中断底层连接，
// Close() 与 Publish 均迅速退出且不遗留阻塞中的底层写操作。
func TestConfirmPublisher_CloseUnblocksInFlightBlockedPublish(t *testing.T) {
	broker := startFakeAMQPBroker(t, nil)

	var firstConn *blockingWriteConn
	client, err := newClientWithOptions(
		testMQConfig(broker.URL()),
		func(c *Client) {
			c.dialTimeout = time.Second
			c.setupTimeout = time.Second
			c.writeTimeout = 10 * time.Second
			c.closeTimeout = 300 * time.Millisecond
			c.heartbeat = 0
			c.initialBackoff = 50 * time.Millisecond
			c.dialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
				nc, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				if firstConn == nil {
					firstConn = newBlockingWriteConn(nc)
					return firstConn, nil
				}
				return nc, nil
			}
		},
	)
	if err != nil {
		t.Fatalf("newClientWithOptions: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	pub := NewConfirmPublisher(client)
	// 先建好通道
	if err := pub.Publish(context.Background(), Message{
		Exchange: "wallpaper.events", RoutingKey: "post.created", MessageID: "msg-pre", Body: []byte(`{}`),
	}); err != nil {
		t.Fatalf("pre publish failed: %v", err)
	}

	firstConn.blockWrites.Store(true)

	pubDone := make(chan error, 1)
	go func() {
		longCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		pubDone <- pub.Publish(longCtx, Message{
			Exchange: "wallpaper.events", RoutingKey: "post.created", MessageID: "msg-hang", Body: []byte(`{}`),
		})
	}()

	// 等待 Publish 真正进入底层 Write 阻塞。
	select {
	case <-firstConn.writeEnteredCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Publish to enter blocked Write")
	}

	closeStart := time.Now()
	if err := pub.Close(); err != nil {
		t.Fatalf("pub.Close() returned error: %v", err)
	}
	if elapsed := time.Since(closeStart); elapsed > 500*time.Millisecond {
		t.Fatalf("pub.Close() was dragged by blocked Publish: %v", elapsed)
	}

	select {
	case err := <-pubDone:
		if !errors.Is(err, ErrClientClosed) {
			t.Fatalf("expected interrupted Publish to return ErrClientClosed, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("blocked Publish goroutine did not exit after Close()")
	}

	select {
	case <-firstConn.writeExitedCh:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("underlying Write did not unblock after Close()")
	}
}

// TestConfirmPublisher_OpenChannelAndCloseHangBounded 验证：
// 1) Broker 在 Channel.Open 阶段挂起不回包时，Publish 受 ctx 期限约束并触发重连；
// 2) Broker 在 Channel.Close / Connection.Close 阶段不回包时，Publisher.Close() 与 Client.Close() 在期限内退出。
func TestConfirmPublisher_OpenChannelAndCloseHangBounded(t *testing.T) {
	// 场景 A：第 1 次 Channel.Open 用于拓扑声明成功，第 2 次（ConfirmPublisher 建发布通道）挂起。
	beh := &fakeAMQPBehavior{hangChannelOpenAfterN: 2}
	broker := startFakeAMQPBroker(t, beh)

	client, err := newClientWithOptions(
		testMQConfig(broker.URL()),
		func(c *Client) {
			c.dialTimeout = time.Second
			c.setupTimeout = time.Second
			c.closeTimeout = 150 * time.Millisecond
			c.heartbeat = 0
			c.initialBackoff = 30 * time.Millisecond
		},
	)
	if err != nil {
		t.Fatalf("newClientWithOptions: %v", err)
	}
	pub := NewConfirmPublisher(client)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	pubErr := pub.Publish(ctx, Message{
		Exchange: "wallpaper.events", RoutingKey: "post.created", MessageID: "msg-ch-open-hang", Body: []byte(`{}`),
	})
	if elapsed := time.Since(start); elapsed > 800*time.Millisecond {
		t.Fatalf("channel open timeout took too long: %v", elapsed)
	}
	if !IsConnectionError(pubErr) || !errors.Is(pubErr, context.DeadlineExceeded) {
		t.Fatalf("expected connection timeout on hung Channel.Open, got: %v", pubErr)
	}

	// 放行通道建立，等待自动重连后正常发布；再模拟 Close 握手黑洞验证关闭期限。
	beh.mu.Lock()
	beh.hangChannelOpenAfterN = 0
	beh.mu.Unlock()

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	if _, err := client.WaitConnection(waitCtx); err != nil {
		t.Fatalf("reconnect after channel open hang failed: %v", err)
	}
	if err := pub.Publish(waitCtx, Message{
		Exchange: "wallpaper.events", RoutingKey: "post.created", MessageID: "msg-after-open-recover", Body: []byte(`{}`),
	}); err != nil {
		t.Fatalf("publish after channel open recovery failed: %v", err)
	}

	// 场景 B：开启 Channel.Close 与 Connection.Close 黑洞，验证关闭不会卡死。
	beh.mu.Lock()
	beh.hangChannelClose = true
	beh.hangConnectionClose = true
	beh.mu.Unlock()

	closeStart := time.Now()
	_ = pub.Close()
	_ = client.Close()
	if elapsed := time.Since(closeStart); elapsed > 1*time.Second {
		t.Fatalf("bounded Close took too long on hung CloseOk: %v", elapsed)
	}
}

// TestClient_CloseInterruptsHungTopologyDeclare 验证：
// 后台重连在 DeclareTopology 挂起时，Client.Close() 立即打断底层 socket 并快速退出 maintainLoop。
func TestClient_CloseInterruptsHungTopologyDeclare(t *testing.T) {
	entered := make(chan struct{})
	beh := &fakeAMQPBehavior{
		hangExchangeDeclare:    true,
		exchangeDeclareEntered: entered,
	}
	broker := startFakeAMQPBroker(t, beh)

	// 启动 NewClient 并发执行，等待其进入 Exchange.Declare 挂起后直接异步验证 Client.Close() 打断能力：
	// 先让首次建连超时快速进入后台 maintainLoop，再让后台重连卡在 Exchange.Declare。
	beh.mu.Lock()
	beh.hangExchangeDeclare = false
	beh.exchangeDeclareEntered = nil
	beh.mu.Unlock()

	client, err := newClientWithOptions(
		testMQConfig(broker.URL()),
		func(c *Client) {
			c.dialTimeout = time.Second
			c.setupTimeout = 5 * time.Second
			c.closeTimeout = 200 * time.Millisecond
			c.heartbeat = 0
			c.initialBackoff = 20 * time.Millisecond
		},
	)
	if err != nil {
		t.Fatalf("newClientWithOptions: %v", err)
	}

	// 切到 Exchange.Declare 挂起并触发重连。
	beh.mu.Lock()
	beh.hangExchangeDeclare = true
	beh.exchangeDeclareEntered = entered
	beh.mu.Unlock()
	client.ReportError(client.Connection())

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for reconnect to enter Exchange.Declare")
	}

	closeStart := time.Now()
	_ = client.Close()
	if elapsed := time.Since(closeStart); elapsed > 600*time.Millisecond {
		t.Fatalf("Client.Close() blocked by hung topology declare: %v", elapsed)
	}
}

// TestConfirmPublisher_NonConnectionErrorsPreserveHealth 验证 Nack 与 Channel 404 不被误判为连接错误，且不中断健康连接。
func TestConfirmPublisher_NonConnectionErrorsPreserveHealth(t *testing.T) {
	beh := &fakeAMQPBehavior{channelNotFoundOnKey: "missing.exchange.key"}
	broker := startFakeAMQPBroker(t, beh)

	client, err := newClientWithOptions(
		testMQConfig(broker.URL()),
		func(c *Client) {
			c.dialTimeout = time.Second
			c.setupTimeout = time.Second
			c.closeTimeout = 200 * time.Millisecond
			c.heartbeat = 0
		},
	)
	if err != nil {
		t.Fatalf("newClientWithOptions: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	pub := NewConfirmPublisher(client)
	t.Cleanup(func() { _ = pub.Close() })

	// 1. 404 NOT_FOUND 属于非连接错误，且底层连接保持存活。
	connBefore := client.Connection()
	err = pub.Publish(context.Background(), Message{
		Exchange: "bad.exchange", RoutingKey: "missing.exchange.key", MessageID: "msg-404", Body: []byte(`{}`),
	})
	if err == nil {
		t.Fatal("expected error for 404 channel close")
	}
	if IsConnectionError(err) {
		t.Fatalf("expected 404 NOT_FOUND NOT to be classified as connection error, got: %v", err)
	}
	if client.Connection() != connBefore {
		t.Fatal("healthy underlying AMQP connection should not be torn down on channel-level 404")
	}

	// 2. Nack 属于非连接错误。
	beh.mu.Lock()
	beh.nackPublish = true
	beh.mu.Unlock()
	err = pub.Publish(context.Background(), Message{
		Exchange: "wallpaper.events", RoutingKey: "post.created", MessageID: "msg-nack", Body: []byte(`{}`),
	})
	if !errors.Is(err, ErrPublishNacked) || IsConnectionError(err) {
		t.Fatalf("expected non-connection ErrPublishNacked, got: %v", err)
	}

	// 3. 恢复正常确认，后续发布在原连接上直接成功。
	beh.mu.Lock()
	beh.nackPublish = false
	beh.mu.Unlock()
	if err := pub.Publish(context.Background(), Message{
		Exchange: "wallpaper.events", RoutingKey: "post.created", MessageID: "msg-ok", Body: []byte(`{}`),
	}); err != nil {
		t.Fatalf("subsequent healthy publish failed: %v", err)
	}
}

// TestConfirmPublisher_WaitConfirmHangAndQueuedPublishTimeout 验证：
// 1) Broker 收到消息但不回 Basic.Ack 且挂起 Channel.Close 时，Publish 在期限内返回连接超时错误；
// 2) 当另一并发 Publish 正在等待发布锁（sem）时，其独立 ctx 超时会立即返回，不被在途发布拖住。
func TestConfirmPublisher_WaitConfirmHangAndQueuedPublishTimeout(t *testing.T) {
	beh := &fakeAMQPBehavior{
		hangPublishConfirm: true,
	}
	broker := startFakeAMQPBroker(t, beh)

	client, err := newClientWithOptions(
		testMQConfig(broker.URL()),
		func(c *Client) {
			c.dialTimeout = time.Second
			c.setupTimeout = time.Second
			c.closeTimeout = 150 * time.Millisecond
			c.heartbeat = 0
			c.initialBackoff = 30 * time.Millisecond
		},
	)
	if err != nil {
		t.Fatalf("newClientWithOptions: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	// 建连完成后再开启 Channel.Close 黑洞，模拟运行期 Confirm 等待超时且关闭通道不回包。
	beh.mu.Lock()
	beh.hangChannelClose = true
	beh.mu.Unlock()

	pub := NewConfirmPublisher(client)
	t.Cleanup(func() { _ = pub.Close() })

	// 协程 1：较长超时（400ms），卡在等待 Broker Confirm。
	errCh1 := make(chan error, 1)
	go func() {
		ctx1, cancel1 := context.WithTimeout(context.Background(), 400*time.Millisecond)
		defer cancel1()
		errCh1 <- pub.Publish(ctx1, Message{
			Exchange: "wallpaper.events", RoutingKey: "post.created", MessageID: "msg-confirm-hang-1", Body: []byte(`{}`),
		})
	}()

	// 等待协程 1 已到达 Broker（持有 sem 并等待 Confirm）。
	deadline := time.Now().Add(time.Second)
	for broker.behavior.publishedCount.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if broker.behavior.publishedCount.Load() == 0 {
		t.Fatal("timed out waiting for first publish to reach broker")
	}

	// 协程 2：等待 sem，设置更短超时（80ms），应提前返回而不被协程 1 的 400ms 阻塞拖住。
	ctx2, cancel2 := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel2()
	start2 := time.Now()
	err2 := pub.Publish(ctx2, Message{
		Exchange: "wallpaper.events", RoutingKey: "post.created", MessageID: "msg-queued-2", Body: []byte(`{}`),
	})
	elapsed2 := time.Since(start2)
	if !IsConnectionError(err2) || !errors.Is(err2, context.DeadlineExceeded) {
		t.Fatalf("expected queued publish deadline error, got: %v", err2)
	}
	if elapsed2 > 250*time.Millisecond {
		t.Fatalf("queued publish was blocked by in-flight publish: %v", elapsed2)
	}

	// 协程 1 也应在 400ms 期限内退出且不卡在 Channel.Close。
	select {
	case err1 := <-errCh1:
		if !IsConnectionError(err1) || !errors.Is(err1, context.DeadlineExceeded) {
			t.Fatalf("expected confirm hang deadline error, got: %v", err1)
		}
	case <-time.After(time.Second):
		t.Fatal("in-flight publish waiting confirm did not exit within deadline")
	}
}
