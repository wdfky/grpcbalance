package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
)

//type GrpcService string

const (
	HelloService string = "helloservice"
)

func etcdInit(etcdaddrs string) (etcdcli *clientv3.Client, err error) {
	return clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(etcdaddrs, ";"),
		DialTimeout: 2 * time.Second,
	})
}

type etcdResover struct {
	etcdaddrs   string
	etcdCli     *clientv3.Client
	clientConn  resolver.ClientConn
	serviceName string
	scheme      string
}

func NewResolve(scheme, etcdaddr, serviceName string) resolver.Builder {
	return &etcdResover{etcdaddrs: etcdaddr, scheme: scheme, serviceName: serviceName}
}
func (e *etcdResover) Scheme() string {
	return e.scheme
}

//ResolveNow 连接异常之后会掉用这玩意，重新发现一次服务， 个人觉得和build中的watch一个功能
func (e *etcdResover) ResolveNow(opts resolver.ResolveNowOptions) {

}

//ResolveNow 实现接口必要不写好像没啥影响
func (e *etcdResover) Close() {

}

// Build 定义解析器resolver的builder
func (e *etcdResover) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	//target scheme://authority/endpoint_name
	etcdCli, err := etcdInit(e.etcdaddrs)
	if err != nil {
		log.Println("etcdCli init error:", err)
		return nil, err
	}
	e.etcdCli = etcdCli
	e.clientConn = cc
	watch := func() {
		var addrs []resolver.Address
		//先获取服务列表
		prefixKey := e.serviceName
		resp, err := e.etcdCli.KV.Get(context.Background(), prefixKey, clientv3.WithPrefix())
		if err != nil {
			log.Fatal(err)
		}
		for _, addr := range resp.Kvs {
			fmt.Printf("addr: %v\n", addr)
			addrs = append(addrs, resolver.Address{Addr: string(addr.Value)})
		}
		e.clientConn.UpdateState(resolver.State{Addresses: addrs})
		rch := e.etcdCli.Watch(context.Background(), prefixKey)
		for wresp := range rch {
			for _, ev := range wresp.Events {
				switch ev.Type {
				case mvccpb.PUT:
					{
						if !exists(addrs, ev.Kv.String()) {
							addrs = append(addrs, resolver.Address{Addr: string(ev.Kv.Value)})
						}
					}
				case mvccpb.DELETE:
					{
						remove(addrs, string(ev.Kv.Value))
					}
				}
			}
			e.clientConn.UpdateState(resolver.State{
				Addresses: addrs,
			})
		}
	}
	go watch()
	//fmt.Println(1)
	return e, nil
}

func exists(l []resolver.Address, addr string) bool {
	for i := range l {
		if l[i].Addr == addr {
			return true
		}
	}
	return false
}

func remove(s []resolver.Address, addr string) ([]resolver.Address, bool) {
	for i := range s {
		if s[i].Addr == addr {
			s[i] = s[len(s)-1]
			return s[:len(s)-1], true
		}
	}
	return nil, false
}
func main() {
	// 首先是scheme用来标识名字，然后是作者之类的随便写，然后是集群地址比如 etcdv3://dasdasd/127.0.0.1:2379, 这便是请求地址
	registy_endpoints := "etcdv3://123/127.0.0.1:2379" //etcd集群地址
	r := NewResolve("etcdv3", "127.0.0.1:2379", HelloService)
	resolver.Register(r)
	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()
	// conn, err := grpc.DialContext(ctx, registy_endpoints,
	// 	grpc.WithInsecure(),
	// 	grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}}]}`),
	// 	grpc.WithBlock(),
	// )
	conn, err := grpc.Dial(registy_endpoints, grpc.WithBalancerName("round_robin"), grpc.WithInsecure())
	if err != nil {
		log.Println("grpc dial error:", err)
		panic(err)
	}

	//resolver.Resolver
	//conn, err := grpc.Dial("127.0.0.1:1234", grpc.WithInsecure())
	// if err != nil {
	// 	log.Fatal(err)
	// }
	client := NewHelloServiceClient(conn)
	// ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	// defer cancel()
	req, err := client.SayHello(ctx, &String{
		Value: "Hello",
	})
	if err != nil {
		log.Fatal("sayhello faild error :", err)
	}
	fmt.Printf("req: %v\n", req.Value)
}
