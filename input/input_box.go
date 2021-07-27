package input

import (
	"reflect"
	"sync"

	"github.com/golang/glog"
	"github.com/kevinu2/gohangout/field_setter"
	"github.com/kevinu2/gohangout/filter"
	"github.com/kevinu2/gohangout/output"
	"github.com/kevinu2/gohangout/topology"
	"github.com/kevinu2/gohangout/value_render"
)

type Box struct {
	config             map[string]interface{} // whole config
	input              topology.Input
	outputsInAllWorker [][]*topology.OutputBox
	stop               bool
	once               sync.Once
	shutdownChan       chan bool

	shutdownWhenNil    bool
	mainThreadExitChan chan struct{}

	addFields map[field_setter.FieldSetter]value_render.ValueRender
}

// SetShutdownWhenNil is used for benchmark.
// Gohangout main thread would exit when one input box receive a nil message, such as Ctrl-D in Stdin input
func (b *Box) SetShutdownWhenNil(shutdownWhenNil bool) {
	b.shutdownWhenNil = shutdownWhenNil
}

func NewInputBox(input topology.Input, inputConfig map[interface{}]interface{}, config map[string]interface{}, mainThreadExitChan chan struct{}) *Box {
	b := &Box{
		input:        input,
		config:       config,
		stop:         false,
		shutdownChan: make(chan bool, 1),

		mainThreadExitChan: mainThreadExitChan,
	}
	if addFields, ok := inputConfig["add_fields"]; ok {
		b.addFields = make(map[field_setter.FieldSetter]value_render.ValueRender)
		for k, v := range addFields.(map[interface{}]interface{}) {
			fieldSetter := field_setter.NewFieldSetter(k.(string))
			if fieldSetter == nil {
				glog.Errorf("could build field setter from %s", k.(string))
				return nil
			}
			b.addFields[fieldSetter] = value_render.GetValueRender(v.(string))
		}
	} else {
		b.addFields = nil
	}
	return b
}

func (b *Box) beat(workerIdx int) {
	var firstNode = b.buildTopology(workerIdx)

	var (
		event map[string]interface{}
	)

	for !b.stop {
		event = b.input.ReadOneEvent()
		if event == nil {
			glog.V(5).Info("received nil message.")
			if b.stop {
				break
			}
			if b.shutdownWhenNil {
				glog.Info("received nil message. shutdown...")
				b.mainThreadExitChan <- struct{}{}
				break
			} else {
				continue
			}
		}
		for fs, v := range b.addFields {
			event = fs.SetField(event, v.Render(event), "", false)
		}
		firstNode.Process(event)
	}
}

func (b *Box) buildTopology(workerIdx int) *topology.ProcessorNode {
	outputs := topology.BuildOutputs(b.config, output.BuildOutput)
	b.outputsInAllWorker[workerIdx] = outputs

	var outputProcessor topology.Processor
	if len(outputs) == 1 {
		outputProcessor = outputs[0]
	} else {
		outputProcessor = (topology.OutputsProcessor)(outputs)
	}

	filterBoxes := topology.BuildFilterBoxes(b.config, filter.BuildFilter)

	var firstNode *topology.ProcessorNode
	for _, b := range filterBoxes {
		firstNode = topology.AppendProcessorsToLink(firstNode, b)
	}
	firstNode = topology.AppendProcessorsToLink(firstNode, outputProcessor)

	// Set BelongTo
	var node *topology.ProcessorNode
	node = firstNode
	for _, b := range filterBoxes {
		node = node.Next
		v := reflect.ValueOf(b.Filter)
		f := v.MethodByName("SetBelongTo")
		if f.IsValid() {
			f.Call([]reflect.Value{reflect.ValueOf(node)})
		}
	}

	return firstNode
}

func (b *Box) Beat(worker int) {
	b.outputsInAllWorker = make([][]*topology.OutputBox, worker)
	for i := 0; i < worker; i++ {
		go b.beat(i)
	}

	<-b.shutdownChan
}

func (b *Box) shutdown() {
	b.once.Do(func() {

		glog.Infof("try to shutdown input %T", b.input)
		b.input.Shutdown()

		for i, outputs := range b.outputsInAllWorker {
			for _, o := range outputs {
				glog.Infof("try to shutdown output %T in worker %d", o, i)
				o.Output.Shutdown()
			}
		}
	})

	b.shutdownChan <- true
}

func (b *Box) Shutdown() {
	b.shutdown()
	b.stop = true
}
