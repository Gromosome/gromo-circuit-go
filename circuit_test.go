package circuit
import("context";"errors";"sync/atomic";"testing";"time")
func TestOpensAndRunsFallbackInGoroutine(t *testing.T){c:=New(Config{ID:"db",FailureThreshold:1,OpenTimeout:time.Hour},nil);var called atomic.Bool;err:=c.Execute(context.Background(),func(context.Context)error{return errors.New("down")},func(error){called.Store(true)});if err==nil||c.State()!=Open{t.Fatal("expected open circuit")};deadline:=time.Now().Add(time.Second);for !called.Load()&&time.Now().Before(deadline){time.Sleep(time.Millisecond)};if !called.Load(){t.Fatal("fallback was not run")}}
