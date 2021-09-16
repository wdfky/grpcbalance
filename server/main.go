package main

import (
	context "context"
	"log"
	"net"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"google.golang.org/grpc"
)

type Hello struct{}
type ServiceInfo struct {
	Name string
	Addr string
}
type Service struct {
	ServiceInfo ServiceInfo
	stop        chan error
	leaseId     clientv3.LeaseID
	client      *clientv3.Client
}

const (
	HelloService string = "helloservice"
)

func (h *Hello) SayHello(ctx context.Context, req *String) (ret *String, err error) {
	ret = &String{Value: req.GetValue() + " World"}
	return
}
func (h *Hello) mustEmbedUnimplementedHelloServiceServer() {}
func (s *Service) RegisterEtcd(etcdaddr, servicename, serviceaddr string, ttl int64) (err error) {
	s.ServiceInfo = ServiceInfo{
		Name: servicename,
		Addr: serviceaddr,
	}
	s.client, err = clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(etcdaddr, ";"),
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		return err
	}
	go func() {
		ch, err := s.keepLive(ttl)
		if err != nil {
			log.Fatal(err)
		}
		for {
			select {
			case <-s.stop:
				return
			case <-s.client.Ctx().Done():
				log.Println("etcd close")
				return
			case resp, ok := <-ch:
				if !ok {
					log.Println("keepalive channel close")
					err = s.revoke()
					if err != nil {
						log.Println("revoke err")
					}
					return
				}
				log.Printf("Recv reply from service: %s, ttl:%d", s.ServiceInfo.Name+"://"+s.ServiceInfo.Addr, resp.TTL)
			}
		}
	}()
	return nil
}
func (s *Service) Start() (err error) {
	return
}
func (s *Service) keepLive(ttl int64) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	key := s.ServiceInfo.Name + "://" + s.ServiceInfo.Addr
	lresp, err := s.client.Grant(context.TODO(), ttl)
	if err != nil {
		log.Fatal(err)
	}
	s.client.KV.Put(context.TODO(), key, s.ServiceInfo.Addr, clientv3.WithLease(lresp.ID))
	s.leaseId = lresp.ID
	return s.client.KeepAlive(context.TODO(), s.leaseId)
}

func (s *Service) revoke() error {
	_, err := s.client.Revoke(context.TODO(), s.leaseId)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("servide:%s stop\n", s.ServiceInfo.Name)
	return err
}
func main() {
	grpcServer := grpc.NewServer()
	RegisterHelloServiceServer(grpcServer, new(Hello))
	lis, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal(err)
	}

	helloService := &Service{}
	err = helloService.RegisterEtcd("127.0.0.1:2379", HelloService, "127.0.0.1:1234", 5)
	if err != nil {
		log.Fatal(err)
	}
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatal(err)
	}
}
