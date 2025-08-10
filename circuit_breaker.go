package norm

import (
	"errors"
	"sync"
	"time"
)

// simple circuit breaker implementation (closed -> open -> half-open -> closed)

type circuitState int

const (
	stateClosed circuitState = iota
	stateOpen
	stateHalfOpen
)

type circuitBreakerConfig struct {
	failureThreshold    int
	openTimeout         time.Duration
	halfOpenMaxInFlight int
	onStateChange       func(state string)
}

type circuitBreaker struct {
	mu          sync.Mutex
	state       circuitState
	failures    int
	openedAt    time.Time
	cfg         circuitBreakerConfig
	halfOpenSem chan struct{}
}

var circuitOpenErr = errors.New("circuit breaker is open")

func isCircuitOpenError(err error) bool { return errors.Is(err, circuitOpenErr) }

func newCircuitBreaker(cfg circuitBreakerConfig) *circuitBreaker {
	if cfg.halfOpenMaxInFlight <= 0 {
		cfg.halfOpenMaxInFlight = 1
	}
	cb := &circuitBreaker{state: stateClosed, cfg: cfg}
	cb.halfOpenSem = make(chan struct{}, cfg.halfOpenMaxInFlight)
	return cb
}

func (cb *circuitBreaker) setState(s circuitState) {
	if cb.state == s {
		return
	}
	cb.state = s
	switch s {
	case stateClosed:
		cb.failures = 0
		if cb.cfg.onStateChange != nil {
			cb.cfg.onStateChange("closed")
		}
	case stateOpen:
		cb.openedAt = time.Now()
		if cb.cfg.onStateChange != nil {
			cb.cfg.onStateChange("open")
		}
	case stateHalfOpen:
		// reset semaphore
		cb.halfOpenSem = make(chan struct{}, cb.cfg.halfOpenMaxInFlight)
		if cb.cfg.onStateChange != nil {
			cb.cfg.onStateChange("half_open")
		}
	}
}

// before must be called right before an operation is attempted
func (cb *circuitBreaker) before() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case stateClosed:
		return nil
	case stateOpen:
		if time.Since(cb.openedAt) >= cb.cfg.openTimeout {
			cb.setState(stateHalfOpen)
		} else {
			return circuitOpenErr
		}
	}
	// half-open state: limit in-flight trial calls
	select {
	case cb.halfOpenSem <- struct{}{}:
		return nil
	default:
		return circuitOpenErr
	}
}

// after must be called exactly once after an operation completes
func (cb *circuitBreaker) after(opErr error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// release half-open semaphore if needed
	if cb.state == stateHalfOpen {
		select {
		case <-cb.halfOpenSem:
		default:
		}
	}

	if opErr == nil {
		switch cb.state {
		case stateClosed:
			cb.failures = 0
		case stateHalfOpen:
			// successful trial -> close circuit
			cb.setState(stateClosed)
		case stateOpen:
			// ignore (should not happen)
		}
		return
	}
	// On error, count failures only in closed or half-open
	switch cb.state {
	case stateClosed:
		cb.failures++
		if cb.failures >= cb.cfg.failureThreshold {
			cb.setState(stateOpen)
		}
	case stateHalfOpen:
		// failed trial -> open again
		cb.setState(stateOpen)
	}
}
