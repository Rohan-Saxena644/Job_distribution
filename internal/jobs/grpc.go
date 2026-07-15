package jobs

import (
	"context"
	"crypto/subtle"
	"fmt"
	"log"
	"math"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

const submitJobMethod = "/jobs.v1.JobService/SubmitJob"

type GRPCServerConfig struct {
	Address       string
	TLSCertFile   string
	TLSKeyFile    string
	AuthToken     string
	AllowInsecure bool
}

type GRPCClientConfig struct {
	Address       string
	TLSCAFile     string
	TLSServerName string
	AuthToken     string
}

type jobGRPCService interface {
	SubmitJob(ctx context.Context, request *structpb.Struct) (*structpb.Struct, error)
}

type jobGRPCServer struct {
	service *Service
}

func StartGRPCServer(ctx context.Context, config GRPCServerConfig, service *Service) error {
	serverOptions := []grpc.ServerOption{}
	if !config.AllowInsecure && (config.TLSCertFile == "" || config.TLSKeyFile == "" || config.AuthToken == "") {
		return fmt.Errorf("gRPC API requires a TLS certificate, private key, and authentication token")
	}
	if (config.TLSCertFile == "") != (config.TLSKeyFile == "") {
		return fmt.Errorf("both gRPC TLS certificate and key are required")
	}
	if config.TLSCertFile != "" {
		tlsCredentials, err := credentials.NewServerTLSFromFile(config.TLSCertFile, config.TLSKeyFile)
		if err != nil {
			return err
		}
		serverOptions = append(serverOptions, grpc.Creds(tlsCredentials))
	}
	if config.AuthToken != "" {
		if config.TLSCertFile == "" {
			return fmt.Errorf("gRPC authentication requires TLS")
		}
		serverOptions = append(serverOptions, grpc.UnaryInterceptor(authenticateGRPC(config.AuthToken)))
	}

	listener, err := net.Listen("tcp", config.Address)
	if err != nil {
		return err
	}

	server := grpc.NewServer(serverOptions...)
	server.RegisterService(&jobServiceDescription, &jobGRPCServer{service: service})

	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()

	log.Println("gRPC API listening on", config.Address)
	return server.Serve(listener)
}

func (s *jobGRPCServer) SubmitJob(ctx context.Context, request *structpb.Struct) (*structpb.Struct, error) {
	input, err := grpcRequestToJobInput(request)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	job, err := s.service.SubmitJob(ctx, input)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return jobToGRPCResponse(job)
}

func SubmitJobGRPC(ctx context.Context, config GRPCClientConfig, input SubmitJobInput) (Job, error) {
	transportCredentials := credentials.TransportCredentials(insecure.NewCredentials())
	if config.TLSCAFile != "" {
		tlsCredentials, err := credentials.NewClientTLSFromFile(config.TLSCAFile, config.TLSServerName)
		if err != nil {
			return Job{}, err
		}
		transportCredentials = tlsCredentials
	} else if config.AuthToken != "" {
		return Job{}, fmt.Errorf("gRPC authentication requires TLS")
	}

	connection, err := grpc.NewClient(config.Address, grpc.WithTransportCredentials(transportCredentials))
	if err != nil {
		return Job{}, err
	}
	defer connection.Close()

	request, err := jobInputToGRPCRequest(input)
	if err != nil {
		return Job{}, err
	}

	response := &structpb.Struct{}
	if config.AuthToken != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+config.AuthToken)
	}
	if err := connection.Invoke(ctx, submitJobMethod, request, response); err != nil {
		return Job{}, err
	}

	fields := response.AsMap()
	return Job{
		ID:             int(fields["id"].(float64)),
		IdempotencyKey: fields["idempotency_key"].(string),
		Type:           JobType(fields["type"].(string)),
		Status:         JobStatus(fields["status"].(string)),
		Priority:       JobPriority(fields["priority"].(string)),
	}, nil
}

func grpcRequestToJobInput(request *structpb.Struct) (SubmitJobInput, error) {
	fields := request.AsMap()
	jobType, _ := fields["type"].(string)
	if jobType == "" {
		return SubmitJobInput{}, fmt.Errorf("type is required")
	}
	idempotencyKey, _ := fields["idempotency_key"].(string)
	if idempotencyKey == "" {
		return SubmitJobInput{}, fmt.Errorf("idempotency_key is required")
	}

	priority, _ := fields["priority"].(string)
	if priority != "" && priority != string(JobPriorityLow) && priority != string(JobPriorityMedium) && priority != string(JobPriorityHigh) {
		return SubmitJobInput{}, fmt.Errorf("priority must be low, medium, or high")
	}

	payload := map[string]string{}
	if rawPayload, exists := fields["payload"]; exists {
		payloadValues, ok := rawPayload.(map[string]any)
		if !ok {
			return SubmitJobInput{}, fmt.Errorf("payload must be an object")
		}
		for key, value := range payloadValues {
			payload[key] = fmt.Sprint(value)
		}
	}

	maxRetries := 0
	if rawValue, exists := fields["max_retries"]; exists {
		value, ok := rawValue.(float64)
		if !ok || math.Trunc(value) != value {
			return SubmitJobInput{}, fmt.Errorf("max_retries must be a whole number")
		}
		maxRetries = int(value)
		if maxRetries < 0 {
			return SubmitJobInput{}, fmt.Errorf("max_retries cannot be negative")
		}
	}

	var scheduledAt *time.Time
	if value, ok := fields["scheduled_at"].(string); ok && value != "" {
		parsed, err := time.Parse(time.RFC3339, value)
		if err != nil {
			return SubmitJobInput{}, fmt.Errorf("scheduled_at must use RFC3339 format")
		}
		scheduledAt = &parsed
	}

	return SubmitJobInput{
		IdempotencyKey: idempotencyKey,
		Type:           JobType(jobType),
		Payload:        payload,
		Priority:       JobPriority(priority),
		ScheduledAt:    scheduledAt,
		MaxRetries:     maxRetries,
	}, nil
}

func jobInputToGRPCRequest(input SubmitJobInput) (*structpb.Struct, error) {
	payload := make(map[string]any, len(input.Payload))
	for key, value := range input.Payload {
		payload[key] = value
	}

	request := map[string]any{
		"idempotency_key": input.IdempotencyKey,
		"type":            string(input.Type),
		"payload":         payload,
		"priority":        string(input.Priority),
		"max_retries":     input.MaxRetries,
	}
	if input.ScheduledAt != nil {
		request["scheduled_at"] = input.ScheduledAt.Format(time.RFC3339)
	}

	return structpb.NewStruct(request)
}

func jobToGRPCResponse(job Job) (*structpb.Struct, error) {
	return structpb.NewStruct(map[string]any{
		"id":              job.ID,
		"idempotency_key": job.IdempotencyKey,
		"type":            string(job.Type),
		"status":          string(job.Status),
		"priority":        string(job.Priority),
	})
}

func authenticateGRPC(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		values := metadata.ValueFromIncomingContext(ctx, "authorization")
		expected := "Bearer " + token
		if len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte(expected)) != 1 {
			return nil, status.Error(codes.Unauthenticated, "invalid authentication token")
		}

		return handler(ctx, request)
	}
}

func submitJobHandler(server any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	request := &structpb.Struct{}
	if err := decode(request); err != nil {
		return nil, err
	}

	if interceptor == nil {
		return server.(jobGRPCService).SubmitJob(ctx, request)
	}

	info := &grpc.UnaryServerInfo{
		Server:     server,
		FullMethod: submitJobMethod,
	}
	handler := func(ctx context.Context, request any) (any, error) {
		return server.(jobGRPCService).SubmitJob(ctx, request.(*structpb.Struct))
	}

	return interceptor(ctx, request, info, handler)
}

var jobServiceDescription = grpc.ServiceDesc{
	ServiceName: "jobs.v1.JobService",
	HandlerType: (*jobGRPCService)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "SubmitJob",
			Handler:    submitJobHandler,
		},
	},
	Metadata: "api/jobs.proto",
}
