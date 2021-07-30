
//+build !windows,!plan9

package input

import (
	"github.com/golang/glog"
	"github.com/kevinu2/gohangout/codec"
	"github.com/kevinu2/gohangout/common"
	"github.com/kevinu2/gohangout/topology"
	"github.com/kevinu2/shm/ishm"
	"sync"
	"time"
)

type ShmMessage struct {
	TopicName   string
	EventType   string
	Tag int64
	Value     []byte
	Error       error
}

type ShmInput struct {
	config  map[interface{}]interface{}
	decoder codec.Decoder
	shmKey  int64
	LastSuccessConsumer *ishm.Consumer
	messages chan *ShmMessage
	messagesLength int
	decorateEvents bool
	sync.Mutex
}

const  (
	eventTypeMaxLen = 30
	topicNameMaxLen = 30
	contentMaxLen = 40960
	workingPoolSize = 500
)

var (
    workingPool = common.NewEmptyPool()
)

func init() {
	pool, err := common.NewPool(workingPoolSize, common.PoolOption{
		ExpiryDuration: time.Minute * 2,
	})
	if err != nil {
		glog.Error(err)
	} else {
		workingPool = pool
	}
	Register("Shm", newShmInput)
}

func newShmInput(config map[interface{}]interface{}) topology.Input {
	var coderType = "plain"
	if codecV, ok := config["codec"]; ok {
		coderType = codecV.(string)
	}

	messagesLength := 10
	if v, ok := config["messages_queue_length"]; ok {
		messagesLength = v.(int)
	}

	decorateEvents := false
	if decorateEventsV, ok := config["decorate_events"]; ok {
		decorateEvents = decorateEventsV.(bool)
	}

	var shmKey int64 = 999999
	v, ok := config["shm_key"]
	if !ok || v.(int) < 0 {
       glog.Fatal("shm_key must be configured in Shm Input and shm_key must be greater than 0")
    }
	shmKey = int64(v.(int))
	p := &ShmInput{
		config:  config,
		decoder:  codec.NewDecoder(coderType),
		shmKey:  shmKey,
		decorateEvents: decorateEvents,
		messages: make(chan *ShmMessage, messagesLength),
	}
	p.connectToShmMemory(nil)
	return p
}

func (p *ShmInput) connectToShmMemory(consumer *ishm.Consumer) {
	p.Lock()
	defer p.Unlock()
	if consumer != nil && p.LastSuccessConsumer != consumer {
		return
	}
	shmKey := p.shmKey
	glog.Infof("[try to connect share memory with the ShmKey %v...]", shmKey)
	for !ishm.HasKeyofSHM(shmKey) {
		glog.Warningf("try to read share memory with the ShmKey %v, waiting...", shmKey)
		time.Sleep(time.Second * 5)
	}
	var shareMemory *ishm.SHMInfo
	for {
		tempShm, err := ishm.GetShareMemoryInfo(shmKey, false)
		if err != nil {
			time.Sleep(time.Second * 5)
			glog.Error(err)
			continue
		} else {
			if tempShm.Count == 0 {
				time.Sleep(time.Second * 5)
				glog.Warningf("try to read share memory with the ShmKey %v, waiting...", shmKey)
				continue
			} else {
				glog.Infof("consume share memory with the ShmKey %v success", shmKey)
				shareMemory = tempShm
				break
			}
		}
	}
	count := int(shareMemory.Count)
	if count > 200 {
		count = 200
	}
	for i := 0; i < count; i++ {
		consumerKey := shareMemory.Key[i]
		if consumerKey > 0 {
			go p.startShmConsumer(shareMemory, consumerKey)
		}
	}
}

func readAndSendTLVData(tlv *ishm.TagTLV, msgChan chan *ShmMessage)  {
	if tlv.EventTypeLen > eventTypeMaxLen {
		glog.Warningf("event type len is too large with the length %d exceed the max value %d", tlv.EventTypeLen, eventTypeMaxLen)
		tlv.EventTypeLen = eventTypeMaxLen
	}
	if tlv.Len > contentMaxLen {
		glog.Warningf("event content len is too large with the length %d exceed the max value %d", tlv.Len, contentMaxLen)
		tlv.Len = contentMaxLen
	}
	if tlv.TopicLen > topicNameMaxLen {
		glog.Warningf("topic name len is too large with the length %d exceed the max value %d", tlv.TopicLen, topicNameMaxLen)
		tlv.TopicLen = topicNameMaxLen
	}
	msg := new(ShmMessage)
	//msg.TopicName = string(tlv.Topic[:tlv.TopicLen])
	msg.Tag = tlv.Tag
	msg.Value = tlv.Value[:tlv.Len]
	//msg.EventType = string(tlv.EventType[:tlv.EventTypeLen])
	msgChan <- msg
}

func (p *ShmInput) startShmConsumer(shareMemory *ishm.SHMInfo, key int32)  {
	consumer := new(ishm.Consumer)
	success := consumer.Init(int64(key), shareMemory.MaxSHMSize, shareMemory.MaxContentLen)
	if !success {
		glog.Errorf("init consumer error with shm key = %d and  consumer key = %d", p.shmKey, key)
		return
	}
	glog.Infof("init consumer success with shm key = %d and consumer key = %d", p.shmKey, key)
	p.LastSuccessConsumer = consumer
	for  {
		tlv, status := consumer.Next()
		switch status {
		case ishm.ShmConsumerOk:
			workingPool.Submit(func() {
				readAndSendTLVData(tlv, p.messages)
			})
			continue
		case ishm.ShmConsumerNoData:
			time.Sleep(time.Microsecond * 500)
			continue
		case ishm.ShmConsumerLenErr:
			glog.Error("len error")
		case ishm.ShmConsumerReadErr:
			glog.Error("read error, please check log producer is alive")
			consumer.Reset()
			p.connectToShmMemory(consumer)
			return
		case ishm.ShmConsumerInitErr:
			glog.Error("init error")
		default:
			glog.Error("unknown error")
		}
		consumer.Reset()
		time.Sleep(time.Second * 5)
	}
}

func (p *ShmInput) ReadOneEvent() map[string]interface{} {
	message, more := <-p.messages
	if !more {
		return nil
	}
	if message.Error != nil {
		glog.Error("shm message carries error: ", message.Error)
		return nil
	}
	event := p.decoder.Decode(message.Value)
	if p.decorateEvents {
		shmMeta := make(map[string]interface{})
		shmMeta["topic"] = message.TopicName
		shmMeta["tag"] = message.Tag
		shmMeta["shm_key"] = p.shmKey
		shmMeta["event_type"] = message.EventType
		event["@metadata"] = map[string]interface{}{"shm": shmMeta}
	}
	return event
}

func (p *ShmInput) Shutdown() {}
