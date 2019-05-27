package yamux

import (
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-buffer-pool"
)

type streamState int

const (
	streamInit streamState = iota
	streamSYNSent
	streamSYNReceived
	streamEstablished
	streamFinished
)

type halfStreamState int

const (
	halfOpen halfStreamState = iota
	halfClosed
	halfReset
)

// Stream is used to represent a logical stream
// within a session.
type Stream struct {
	recvWindow uint32
	sendWindow uint32

	id      uint32
	session *Session

	state                 streamState
	writeState, readState halfStreamState
	stateLock             sync.Mutex

	recvLock sync.Mutex
	recvBuf  pool.Buffer

	sendLock sync.Mutex

	recvNotifyCh chan struct{}
	sendNotifyCh chan struct{}

	readDeadline, writeDeadline pipeDeadline
}

// newStream is used to construct a new stream within
// a given session for an ID
func newStream(session *Session, id uint32, state streamState) *Stream {
	s := &Stream{
		id:            id,
		session:       session,
		state:         state,
		recvWindow:    initialStreamWindow,
		sendWindow:    initialStreamWindow,
		readDeadline:  makePipeDeadline(),
		writeDeadline: makePipeDeadline(),
		recvNotifyCh:  make(chan struct{}, 1),
		sendNotifyCh:  make(chan struct{}, 1),
	}
	return s
}

// Session returns the associated stream session
func (s *Stream) Session() *Session {
	return s.session
}

// StreamID returns the ID of this stream
func (s *Stream) StreamID() uint32 {
	return s.id
}

// Read is used to read from the stream
func (s *Stream) Read(b []byte) (n int, err error) {
	defer asyncNotify(s.recvNotifyCh)
START:
	s.stateLock.Lock()
	switch s.readState {
	case halfOpen:
	case halfClosed:
		s.recvLock.Lock()
		canRead := s.recvBuf.Len() > 0
		s.recvLock.Unlock()
		if !canRead {
			s.stateLock.Unlock()
			return 0, io.EOF
		}
	case halfReset:
		s.stateLock.Unlock()
		return 0, ErrConnectionReset
	default:
		panic("unknown state")
	}
	s.stateLock.Unlock()

	// If there is no data available, block
	s.recvLock.Lock()
	if s.recvBuf.Len() == 0 {
		s.recvLock.Unlock()
		goto WAIT
	}

	// Read any bytes
	n, _ = s.recvBuf.Read(b)
	s.recvLock.Unlock()

	// Send a window update potentially
	err = s.sendWindowUpdate()
	return n, err

WAIT:
	select {
	case <-s.recvNotifyCh:
		goto START
	case <-s.readDeadline.wait():
		return 0, ErrTimeout
	}
}

// Write is used to write to the stream
func (s *Stream) Write(b []byte) (n int, err error) {
	s.sendLock.Lock()
	defer s.sendLock.Unlock()
	total := 0

	for total < len(b) {
		n, err := s.write(b[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// write is used to write to the stream, may return on
// a short write.
func (s *Stream) write(b []byte) (n int, err error) {
	var flags uint16
	var max uint32
	var hdr header

START:
	s.stateLock.Lock()
	switch s.writeState {
	case halfOpen:
	case halfClosed:
		s.stateLock.Unlock()
		return 0, ErrStreamClosed
	case halfReset:
		s.stateLock.Unlock()
		return 0, ErrConnectionReset
	default:
		panic("unknown state")
	}
	s.stateLock.Unlock()

	// If there is no data available, block
	window := atomic.LoadUint32(&s.sendWindow)
	if window == 0 {
		goto WAIT
	}

	// Determine the flags if any
	flags = s.sendFlags()

	// Send up to min(message, window
	max = min(window, s.session.config.MaxMessageSize-headerSize, uint32(len(b)))

	// Send the header
	hdr = encode(typeData, flags, s.id, max)
	if err = s.session.sendMsg(hdr, b[:max], s.writeDeadline.wait()); err != nil {
		return 0, err
	}

	// Reduce our send window
	atomic.AddUint32(&s.sendWindow, ^uint32(max-1))

	// Unlock
	return int(max), err

WAIT:
	select {
	case <-s.sendNotifyCh:
		goto START
	case <-s.writeDeadline.wait():
		return 0, ErrTimeout
	}
}

// sendFlags determines any flags that are appropriate
// based on the current stream state
func (s *Stream) sendFlags() uint16 {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	var flags uint16
	switch s.state {
	case streamInit:
		flags |= flagSYN
		s.state = streamSYNSent
	case streamSYNReceived:
		flags |= flagACK
		s.state = streamEstablished
	}
	return flags
}

// sendWindowUpdate potentially sends a window update enabling
// further writes to take place. Must be invoked with the lock.
func (s *Stream) sendWindowUpdate() error {
	// Determine the delta update
	max := s.session.config.MaxStreamWindowSize
	s.recvLock.Lock()
	delta := (max - uint32(s.recvBuf.Len())) - s.recvWindow

	// Determine the flags if any
	flags := s.sendFlags()

	// Check if we can omit the update
	if delta < (max/2) && flags == 0 {
		s.recvLock.Unlock()
		return nil
	}

	// Update our window
	s.recvWindow += delta
	s.recvLock.Unlock()

	// Send the header
	hdr := encode(typeWindowUpdate, flags, s.id, delta)
	if err := s.session.sendMsg(hdr, nil, nil); err != nil {
		return err
	}
	return nil
}

// sendClose is used to send a FIN
func (s *Stream) sendClose() error {
	flags := s.sendFlags()
	flags |= flagFIN
	hdr := encode(typeWindowUpdate, flags, s.id, 0)
	return s.session.sendMsg(hdr, nil, nil)
}

// sendReset is used to send a RST
func (s *Stream) sendReset() error {
	hdr := encode(typeWindowUpdate, flagRST, s.id, 0)
	return s.session.sendMsg(hdr, nil, nil)
}

// Reset resets the stream (forcibly closes the stream)
func (s *Stream) Reset() error {
	sendReset := false
	s.stateLock.Lock()
	switch s.state {
	case streamFinished:
		s.stateLock.Unlock()
		return nil
	case streamInit:
	case streamSYNSent, streamSYNReceived, streamEstablished:
		sendReset = true
	default:
		panic("unhandled state")
	}

	// at least one direction is open, we need to reset.

	// If we've already sent/received an EOF, no need to reset that side.
	if s.writeState == halfOpen {
		s.writeState = halfReset
	}
	if s.readState == halfOpen {
		s.readState = halfReset
	}
	s.state = streamFinished
	s.notifyWaiting()
	s.stateLock.Unlock()
	if sendReset {
		_ = s.sendReset()
	}
	s.cleanup()
	return nil
}

// CloseWrite is used to close the stream for writing.
func (s *Stream) CloseWrite() error {
	s.stateLock.Lock()
	switch s.writeState {
	case halfOpen:
	case halfClosed:
		s.stateLock.Unlock()
		return nil
	case halfReset:
		s.stateLock.Unlock()
		return ErrConnectionReset
	default:
		panic("invalid state")
	}
	s.writeState = halfClosed
	cleanup := s.readState != halfOpen
	if cleanup {
		s.state = streamFinished
	}
	s.notifyWaiting()
	s.stateLock.Unlock()

	err := s.sendClose()
	if cleanup {
		// we're fully closed, might as well be nice to the user and
		// free everything early.
		s.cleanup()
	}
	return err
}

// CloseRead is used to close the stream for writing.
func (s *Stream) CloseRead() error {
	cleanup := false
	s.stateLock.Lock()
	switch s.readState {
	case halfOpen:
	case halfClosed, halfReset:
		s.stateLock.Unlock()
		return nil
	default:
		panic("invalid state")
	}
	s.readState = halfReset
	cleanup = s.writeState != halfOpen
	if cleanup {
		s.state = streamFinished
	}
	s.notifyWaiting()
	s.stateLock.Unlock()
	if cleanup {
		// we're fully closed, might as well be nice to the user and
		// free everything early.
		s.cleanup()
	}
	return nil
}

// Close is used to close the stream.
func (s *Stream) Close() error {
	_ = s.CloseRead() // can't fail.
	return s.CloseWrite()
}

// forceClose is used for when the session is exiting
func (s *Stream) forceClose() {
	s.stateLock.Lock()
	if s.readState == halfOpen {
		s.readState = halfReset
	}
	if s.writeState == halfOpen {
		s.writeState = halfReset
	}
	s.state = streamFinished
	s.notifyWaiting()
	s.stateLock.Unlock()

	s.readDeadline.set(time.Time{})
	s.readDeadline.set(time.Time{})
}

// called when fully closed to release any system resources.
func (s *Stream) cleanup() {
	s.session.closeStream(s.id)
	s.readDeadline.set(time.Time{})
	s.readDeadline.set(time.Time{})
}

// processFlags is used to update the state of the stream
// based on set flags, if any. Lock must be held
func (s *Stream) processFlags(flags uint16) error {
	// Close the stream without holding the state lock
	closeStream := false
	defer func() {
		if closeStream {
			s.cleanup()
		}
	}()

	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	if flags&flagACK == flagACK {
		if s.state == streamSYNSent {
			s.state = streamEstablished
		}
		s.session.establishStream(s.id)
	}
	if flags&flagFIN == flagFIN {
		if s.readState == halfOpen {
			s.readState = halfClosed
			if s.writeState != halfOpen {
				// We're now fully closed.
				closeStream = true
				s.state = streamFinished
			}
			s.notifyWaiting()
		}
	}
	if flags&flagRST == flagRST {
		if s.readState == halfOpen {
			s.readState = halfReset
		}
		if s.writeState == halfOpen {
			s.writeState = halfReset
		}
		s.state = streamFinished
		closeStream = true
		s.notifyWaiting()
	}
	return nil
}

// notifyWaiting notifies all the waiting channels
func (s *Stream) notifyWaiting() {
	asyncNotify(s.recvNotifyCh)
	asyncNotify(s.sendNotifyCh)
}

// incrSendWindow updates the size of our send window
func (s *Stream) incrSendWindow(hdr header, flags uint16) error {
	if err := s.processFlags(flags); err != nil {
		return err
	}

	// Increase window, unblock a sender
	atomic.AddUint32(&s.sendWindow, hdr.Length())
	asyncNotify(s.sendNotifyCh)
	return nil
}

// readData is used to handle a data frame
func (s *Stream) readData(hdr header, flags uint16, conn io.Reader) error {
	if err := s.processFlags(flags); err != nil {
		return err
	}

	// Check that our recv window is not exceeded
	length := hdr.Length()
	if length == 0 {
		return nil
	}

	// Wrap in a limited reader
	conn = &io.LimitedReader{R: conn, N: int64(length)}

	// Copy into buffer
	s.recvLock.Lock()

	if length > s.recvWindow {
		s.session.logger.Printf("[ERR] yamux: receive window exceeded (stream: %d, remain: %d, recv: %d)", s.id, s.recvWindow, length)
		return ErrRecvWindowExceeded
	}

	s.recvBuf.Grow(int(length))
	if _, err := io.Copy(&s.recvBuf, conn); err != nil {
		s.session.logger.Printf("[ERR] yamux: Failed to read stream data: %v", err)
		s.recvLock.Unlock()
		return err
	}

	// Decrement the receive window
	s.recvWindow -= length
	s.recvLock.Unlock()

	// Unblock any readers
	asyncNotify(s.recvNotifyCh)
	return nil
}

// SetDeadline sets the read and write deadlines
func (s *Stream) SetDeadline(t time.Time) error {
	if err := s.SetReadDeadline(t); err != nil {
		return err
	}
	if err := s.SetWriteDeadline(t); err != nil {
		return err
	}
	return nil
}

// SetReadDeadline sets the deadline for future Read calls.
func (s *Stream) SetReadDeadline(t time.Time) error {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	if s.readState == halfOpen {
		s.readDeadline.set(t)
	}
	return nil
}

// SetWriteDeadline sets the deadline for future Write calls
func (s *Stream) SetWriteDeadline(t time.Time) error {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	if s.writeState == halfOpen {
		s.writeDeadline.set(t)
	}
	return nil
}

// Shrink is a no-op. The internal buffer automatically shrinks itself.
func (s *Stream) Shrink() {
}
