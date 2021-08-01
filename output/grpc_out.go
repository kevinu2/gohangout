package output

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/kevinu2/gohangout/codec"
	pb "github.com/kevinu2/gohangout/dataProcess/go2py/interactive"
	"github.com/kevinu2/gohangout/topology"
	"google.golang.org/grpc"
	"log"
	"time"
)
type JsonData struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Content string `json:"content"`
}

func OutDataToPy(pyAddress, content string) error {
	// Set up a connection to the server.

	conn, err := grpc.Dial(pyAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Printf("did not connect: %v", err)
		return err
	}
	defer conn.Close()
	c := pb.NewProcessDataClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	jd := JsonData{}
	jd.Type = uuid.New().String()
	jd.Name = "from go client"
	jd.Content = content
	od, _ := json.Marshal(jd)
	dr := pb.DataRequest{}
	dr.Name = uuid.New().String()
	dr.Content = uuid.New().String()
	dr.ProcessType = uuid.New().String()
	dr.Data = string(od)
	r, err := c.SetData(ctx, &dr)

	if err != nil {
		log.Printf("could not SetData: %v", err)
	}
	log.Printf("SetData: %#v", r)
	return err
}

//todo add grpc client  send data to other grpc server


func init() {
	Register("GRPCOutput", newSGRPCOutput)
}

type GRPCOutput struct {
	config  map[interface{}]interface{}
	encoder codec.Encoder
}

func newSGRPCOutput(config map[interface{}]interface{}) topology.Output {
	p := &GRPCOutput{
		config: config,
	}

	if v, ok := config["codec"]; ok {
		p.encoder = codec.NewEncoder(v.(string))
	} else {
		p.encoder = codec.NewEncoder("json")
	}

	return p

}

func (p *GRPCOutput) Emit(event map[string]interface{}) {
	buf, err := p.encoder.Encode(event)
	if err != nil {
		glog.Errorf("marshal %v error:%s", event, err)
	}

	//todo call grpc func ,SetData to process buf data
	fmt.Println(string(buf))
}

func (p *GRPCOutput) Shutdown() {
	//todo close grpc client
}
