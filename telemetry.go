package circuit
import("bytes";"encoding/json";"net/http";"sync";"time")
type Event struct{CircuitID string `json:"circuit_id"`;Service string `json:"service"`;Target string `json:"target"`;State string `json:"state"`;Outcome string `json:"outcome"`;Error string `json:"error,omitempty"`;DurationMS int64 `json:"duration_ms"`;Timestamp time.Time `json:"timestamp"`}
type Reporter struct{endpoint string;client *http.Client;queue chan Event;done chan struct{};once sync.Once}
func NewReporter(endpoint string,buffer int)*Reporter{if buffer<1{buffer=256};r:=&Reporter{endpoint:endpoint,client:&http.Client{Timeout:2*time.Second},queue:make(chan Event,buffer),done:make(chan struct{})};go r.run();return r}
func(r *Reporter)Emit(e Event){select{case r.queue<-e:default:}}
func(r *Reporter)Close(){r.once.Do(func(){close(r.done)})}
func(r *Reporter)run(){for{select{case e:=<-r.queue:body,_:=json.Marshal(e);req,_:=http.NewRequest(http.MethodPost,r.endpoint+"/v1/telemetry",bytes.NewReader(body));req.Header.Set("Content-Type","application/json");if resp,err:=r.client.Do(req);err==nil{resp.Body.Close()};case <-r.done:return}}}
