package input

import (
	"bufio"
	"context"
	"fmt"
	"github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/kevinu2/gohangout/codec"
	pb "github.com/kevinu2/gohangout/dataProcess/go2py/interactive"
	"github.com/kevinu2/gohangout/topology"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"time"
)

type grpcServer struct {
	pb.UnimplementedProcessDataServer
}

// SetData implements interactive.setData
func (s *grpcServer) SetData(ctx context.Context, in *pb.DataRequest) (*pb.DataReply, error) {

	log.Printf("Received: %v", in.GetName())

	//todo in is data to GRPCInput message

	log.Print(in)
	dr := pb.DataReply{}
	dr.Message = uuid.New().String()
	dr.Data = uuid.New().String()
	return &dr, nil
}
func (s *grpcServer) OnProcessResult(ctx context.Context, in *pb.DataRequest) (*pb.DataReply, error) {
	log.Printf("Received: %v", in.GetName())
	log.Print(in)
	//todo in is data to GRPCInput message
	dr := pb.DataReply{}
	dr.Message = uuid.New().String()
	dr.Data = uuid.New().String()
	//todo this will be call other outer

	return &dr, nil
}
func getLocalIp() string{
	var IpAddr string
	addrSlice, err := net.InterfaceAddrs()
	if nil != err {
		IpAddr = "localhost"
		return  IpAddr
	}
	for _, addr := range addrSlice {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if nil != ipnet.IP.To4() {
				IpAddr = ipnet.IP.String()
				return IpAddr

			}
		}
	}
	IpAddr = "localhost"
	return IpAddr
}

func runGoServer(port string) error {
	//port = ":50051"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterProcessDataServer(s, &grpcServer{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

	return err
}

type GRPCInput struct {
	config  map[interface{}]interface{}
	decoder codec.Decoder

	scanner  *bufio.Scanner
	messages chan []byte

	stop bool
}

func init() {

	//todo start grpc server use go func

	// start grpc server

	Register("GRPCInput", newGRPCInput)
}
func getGoGRPCServerEndpoint(etcdServer string )(string,error)  {
	var GoGRPCServerEndPoint string =""
	var err error=nil
	return GoGRPCServerEndPoint,err
}
func newGRPCInput(config map[interface{}]interface{}) topology.Input {
	var coderType = "plain"
	if v, ok := config["codec"]; ok {
		coderType = v.(string)
	}
	fmt.Println(coderType)
	var etcdServer = "127.0.0.1:2379"
	if v, ok := config["etcdserver"]; ok {
		etcdServer = v.(string)
	}
	fmt.Println(etcdServer)
	p := &GRPCInput{

		config:   config,
		decoder:  codec.NewDecoder(coderType),
		scanner:  bufio.NewScanner(os.Stdin),
		messages: make(chan []byte, 10),
	}

	//client for go2python server
	addr,err:=getGoGRPCServerEndpoint(etcdServer)

	log.Println(addr)

	go func() {

		//get ip

		//register  to etcd server endpoint
		//or create self server to rec data ?
		if nil != err {
			runGoServer(addr)
		}
	}()

	//todo start grpc service and register ip or get ip from etcd
	return p
}

func (p *GRPCInput) ReadOneEvent() map[string]interface{} {
	if p.scanner.Scan() {
		//todo get message data from grpc server or call go2py server rpc func
		t := p.scanner.Bytes()
		msg := make([]byte, len(t))

		copy(msg, t)
		return p.decoder.Decode(msg)
	}
	if err := p.scanner.Err(); err != nil {
		glog.Errorf("stdin scan error: %v", err)
	}
	time.Sleep(time.Millisecond * 100)
	return nil
}

func (p *GRPCInput) Shutdown() {
	// what we need is to stop emit new event; close messages or not is not important

	//todo stop grpc server and rm key from etcd
	p.stop = true


}

