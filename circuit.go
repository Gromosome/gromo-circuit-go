package circuit

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrOpen = errors.New("circuit is open")
var ErrBusy = errors.New("circuit is busy")

type State string
const ( Closed State = "closed"; Open State = "open"; HalfOpen State = "half_open" )
type Config struct { ID, Service, Target string; FailureThreshold, MaxConcurrent int; OpenTimeout, RequestTimeout time.Duration }
type Circuit struct { cfg Config; reporter *Reporter; mu sync.Mutex; state State; failures, inFlight int; openedAt time.Time }

func New(cfg Config, reporter *Reporter) *Circuit {
	if cfg.FailureThreshold < 1 { cfg.FailureThreshold = 5 }
	if cfg.MaxConcurrent < 1 { cfg.MaxConcurrent = 100 }
	if cfg.OpenTimeout <= 0 { cfg.OpenTimeout = 30*time.Second }
	if cfg.RequestTimeout <= 0 { cfg.RequestTimeout = 5*time.Second }
	return &Circuit{cfg:cfg,reporter:reporter,state:Closed}
}
func (c *Circuit) Execute(ctx context.Context, operation func(context.Context) error, fallback func(error)) error {
	if err:=c.acquire(); err!=nil { c.emit(outcomeFor(err),err,0); c.runFallback(fallback,err); return err }
	started:=time.Now(); defer c.release()
	callCtx,cancel:=context.WithTimeout(ctx,c.cfg.RequestTimeout); defer cancel()
	err:=operation(callCtx)
	if err!=nil { c.recordFailure(); c.emit("failure",err,time.Since(started)); c.runFallback(fallback,err); return err }
	c.recordSuccess(); c.emit("success",nil,time.Since(started)); return nil
}
func (c *Circuit) acquire() error {
	c.mu.Lock(); defer c.mu.Unlock()
	if c.state==Open { if time.Since(c.openedAt)<c.cfg.OpenTimeout{return ErrOpen}; c.state=HalfOpen }
	if c.inFlight>=c.cfg.MaxConcurrent || (c.state==HalfOpen && c.inFlight>0){return ErrBusy}
	c.inFlight++; return nil
}
func (c *Circuit) release(){c.mu.Lock();c.inFlight--;c.mu.Unlock()}
func (c *Circuit) recordSuccess(){c.mu.Lock();c.state=Closed;c.failures=0;c.mu.Unlock()}
func (c *Circuit) recordFailure(){c.mu.Lock();defer c.mu.Unlock();c.failures++;if c.failures>=c.cfg.FailureThreshold{c.state=Open;c.openedAt=time.Now()}}
func (c *Circuit) runFallback(f func(error),err error){if f!=nil{go func(){defer func(){_ = recover()}();f(err)}()}}
func outcomeFor(err error)string{if errors.Is(err,ErrBusy){return "busy"};return "rejected"}
func (c *Circuit) emit(outcome string,err error,elapsed time.Duration){if c.reporter==nil{return};e:=Event{CircuitID:c.cfg.ID,Service:c.cfg.Service,Target:c.cfg.Target,State:string(c.State()),Outcome:outcome,DurationMS:elapsed.Milliseconds(),Timestamp:time.Now().UTC()};if err!=nil{e.Error=err.Error()};c.reporter.Emit(e)}
func (c *Circuit) State()State{c.mu.Lock();defer c.mu.Unlock();return c.state}
