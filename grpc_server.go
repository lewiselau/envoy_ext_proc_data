package main

import (
	"context"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	//"github.com/golang/protobuf/ptypes/wrappers"

	v3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	pb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	clientv3 "go.etcd.io/etcd/client/v3"

	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	grpcport = flag.String("grpcport", ":18080", "grpcport")
	hs       *health.Server
)

var userName string = ""

var userCall = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "department_user_call_count_celsius",
		Help: "The current user API call count in degrees Celsius.",
	},
	// 指定标签名称
	[]string{"department", "user"},
)

var userDepart map[string]string = map[string]string{
	"sal":   "account",
	"jack":  "account",
	"tom":   "development",
	"jerry": "development",
}
var departName string = ""
var departCount int = 0
var etcdCli *clientv3.Client

const ()

type server struct{}

type healthServer struct{}

func (s *healthServer) Check(ctx context.Context, in *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	log.Printf("Handling grpc Check request + %s", in.String())
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func (s *healthServer) Watch(in *healthpb.HealthCheckRequest, srv healthpb.Health_WatchServer) error {
	return status.Error(codes.Unimplemented, "Watch is not implemented")
}

func prom() {
	// 注册到全局默认注册表中
	prometheus.MustRegister(userCall)

	// 暴露自定义的指标
	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":9090", nil)
}

func (s *server) Process(srv pb.ExternalProcessor_ProcessServer) error {

	log.Println("Got stream:  -->  ")

	ctx := srv.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		req, err := srv.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Unknown, "cannot receive stream request: %v", err)
		}

		resp := &pb.ProcessingResponse{}
		switch v := req.Request.(type) {
		case *pb.ProcessingRequest_RequestHeaders:
			log.Printf("pb.ProcessingRequest_RequestHeaders %v \n", v)
			r := req.Request
			h := r.(*pb.ProcessingRequest_RequestHeaders)
			//log.Printf("Got RequestHeaders.Attributes %v", h.RequestHeaders.Attributes)
			//log.Printf("Got RequestHeaders.Headers %v", h.RequestHeaders.Headers)

			isPOST := false
			for _, n := range h.RequestHeaders.Headers.Headers {
				if n.Key == ":method" && n.Value == "POST" {
					isPOST = true
					break
				}
			}

			for _, n := range h.RequestHeaders.Headers.Headers {
				log.Printf("Header %s %s", n.Key, n.Value)
				if n.Key == "user" && isPOST {
					userName = n.Value
					departName = userDepart[userName]
					userCall.WithLabelValues(departName, userName).Inc()

					// write to etcd
					res, err := etcdCli.Get(context.TODO(), "/api-call-count/"+userName)
					count, _ := strconv.Atoi(string(res.Kvs[0].Value))
					_, err = etcdCli.Put(context.TODO(), "/api-call-count/"+userName, strconv.Itoa(count+1))
					if err != nil {
						log.Println("failed to write to etcd for user: " + userName)
					}

					log.Printf("Processing User Header")
					rhq := &pb.HeadersResponse{
						Response: &pb.CommonResponse{
							HeaderMutation: &pb.HeaderMutation{
								RemoveHeaders: []string{"content-length", "user"},
							},
						},
					}

					resp = &pb.ProcessingResponse{
						Response: &pb.ProcessingResponse_RequestHeaders{
							RequestHeaders: rhq,
						},
						ModeOverride: &v3.ProcessingMode{
							RequestBodyMode:    v3.ProcessingMode_BUFFERED,
							ResponseHeaderMode: v3.ProcessingMode_SKIP,
							ResponseBodyMode:   v3.ProcessingMode_NONE,
						},
					}
				}
			}
			break

		case *pb.ProcessingRequest_RequestBody:

			r := req.Request
			b := r.(*pb.ProcessingRequest_RequestBody)
			log.Printf("   RequestBody: %s", string(b.RequestBody.Body))
			log.Printf("   EndOfStream: %T", b.RequestBody.EndOfStream)
			if b.RequestBody.EndOfStream {

				bytesToSend := append(b.RequestBody.Body, []byte(` baaar `)...)
				resp = &pb.ProcessingResponse{
					Response: &pb.ProcessingResponse_RequestBody{
						RequestBody: &pb.BodyResponse{
							Response: &pb.CommonResponse{
								HeaderMutation: &pb.HeaderMutation{
									SetHeaders: []*core.HeaderValueOption{
										{
											Header: &core.HeaderValue{
												Key:   "Content-Length",
												Value: strconv.Itoa(len(bytesToSend)),
											},
										},
									},
								},
								BodyMutation: &pb.BodyMutation{
									Mutation: &pb.BodyMutation_Body{
										Body: bytesToSend,
									},
								},
							},
						},
					},
					ModeOverride: &v3.ProcessingMode{
						ResponseHeaderMode: v3.ProcessingMode_SEND,
						ResponseBodyMode:   v3.ProcessingMode_NONE,
					},
				}
			}
			break
		case *pb.ProcessingRequest_ResponseHeaders:
			log.Printf("pb.ProcessingRequest_ResponseHeaders %v \n", v)
			r := req.Request
			h := r.(*pb.ProcessingRequest_ResponseHeaders)

			responseSize := 0
			for _, n := range h.ResponseHeaders.Headers.Headers {
				if n.Key == "content-length" {
					responseSize, _ = strconv.Atoi(n.Value)
					break
				}
			}

			log.Println("  Removing access-control-allow-* headers")
			rhq := &pb.HeadersResponse{
				Response: &pb.CommonResponse{
					HeaderMutation: &pb.HeaderMutation{
						RemoveHeaders: []string{"access-control-allow-origin", "access-control-allow-credentials"},
						SetHeaders: []*core.HeaderValueOption{
							{
								Header: &core.HeaderValue{
									Key:   "content-type",
									Value: "text/plain",
								},
							},
							{
								Header: &core.HeaderValue{
									Key:   "content-length",
									Value: strconv.Itoa(responseSize + len([]byte(` qux`))),
								},
							},
						},
					},
				},
			}
			resp = &pb.ProcessingResponse{
				Response: &pb.ProcessingResponse_ResponseHeaders{
					ResponseHeaders: rhq,
				},
				ModeOverride: &v3.ProcessingMode{
					ResponseBodyMode: v3.ProcessingMode_BUFFERED,
				},
			}
			break
		case *pb.ProcessingRequest_ResponseBody:
			log.Printf("pb.ProcessingRequest_ResponseBody %v \n", v)
			r := req.Request
			b := r.(*pb.ProcessingRequest_ResponseBody)
			if b.ResponseBody.EndOfStream {
				bytesToSend := append(b.ResponseBody.Body, []byte(` qux`)...)
				resp = &pb.ProcessingResponse{
					Response: &pb.ProcessingResponse_ResponseBody{
						ResponseBody: &pb.BodyResponse{
							Response: &pb.CommonResponse{
								BodyMutation: &pb.BodyMutation{
									Mutation: &pb.BodyMutation_Body{
										Body: bytesToSend,
									},
								},
							},
						},
					},
				}
			}

			break
		default:
			log.Printf("Unknown Request type %v\n", v)
		}
		if err := srv.Send(resp); err != nil {
			log.Printf("send error %v", err)
		}
	}
}

func main() {
	// 创建连接
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return
	}
	etcdCli = cli
	defer func(client *clientv3.Client) {
		_ = client.Close()
	}(cli)

	// 查看以/api-call-count/为前缀的
	get, err := cli.Get(context.TODO(), "/api-call-count/", clientv3.WithPrefix())
	if err != nil {
		return
	}
	for _, kv := range get.Kvs {
		index := strings.LastIndex(string(kv.Key), "/")
		userName = string(kv.Key)[index+1:]
		count, _ := strconv.Atoi(string(kv.Value))
		userCall.WithLabelValues(userDepart[userName], userName).Add(float64(count))
	}

	// start http server for prometheus  //localhost:9090/metrics
	go prom()

	flag.Parse()

	lis, err := net.Listen("tcp", *grpcport)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	sopts := []grpc.ServerOption{grpc.MaxConcurrentStreams(1000)}
	s := grpc.NewServer(sopts...)

	pb.RegisterExternalProcessorServer(s, &server{})
	healthpb.RegisterHealthServer(s, &healthServer{})

	log.Printf("Starting gRPC server on port %s\n", *grpcport)

	var gracefulStop = make(chan os.Signal)
	signal.Notify(gracefulStop, syscall.SIGTERM)
	signal.Notify(gracefulStop, syscall.SIGINT)
	go func() {
		sig := <-gracefulStop
		log.Printf("caught sig: %+v", sig)
		log.Println("Wait for 1 second to finish processing")
		time.Sleep(1 * time.Second)
		os.Exit(0)
	}()
	s.Serve(lis)
}
