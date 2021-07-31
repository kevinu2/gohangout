package input

import (
	"github.com/golang/glog"
	"github.com/kevinu2/gohangout/codec"
	"github.com/kevinu2/gohangout/topology"
	"math/rand"
	"strconv"
	"time"
)

type RandomInput struct {
	config  map[interface{}]interface{}
	decoder codec.Decoder

	from int
	to   int

	maxMessages int
	count       int
}

func init() {
	Register("Random", newRandomInput)
}

func newRandomInput(config map[interface{}]interface{}) topology.Input {
	var codertype string = "plain"

	p := &RandomInput{
		config:      config,
		decoder:     codec.NewDecoder(codertype),
		count:       0,
		maxMessages: -1,
	}

	if v, ok := config["from"]; ok {
		p.from = v.(int)
	} else {
		glog.Fatal("from must be configured in Random Input")
	}

	if v, ok := config["to"]; ok {
		p.to = v.(int)
	} else {
		glog.Fatal("to must be configured in Random Input")
	}

	if v, ok := config["max_messages"]; ok {
		p.maxMessages = v.(int)
	}

	return p
}

func (p *RandomInput) ReadOneEvent() map[string]interface{} {
	//if p.maxMessages != -1 && p.count >= p.maxMessages {
	//	return nil
	//}
    time.Sleep(time.Second * 1)
	n := p.from + rand.Intn(2000)
	p.count++
	return p.decoder.Decode([]byte(strconv.Itoa(n)))
}

func (p *RandomInput) Shutdown() {}
