package jobs

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

const submitJobMethod = "/jobs.v1.JobService/SubmitJob"

type jobGRPCService interface {
	SubmitJob(ctx context.Context, request *structpb.Struct) (*structpb.Struct, error)
}

type jobGRPCServer struct {
	service *Service
}

func StartGRPCServer(ctx context.Context, address string, service *Service) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	server := grpc.NewServer()
	server.RegisterService(&jobServiceDescription, &jobGRPCServer{service: service})

	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()

	log.Println("gRPC API listening on", address)
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

func SubmitJobGRPC(ctx context.Context, address string, input SubmitJobInput) (Job, error) {
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return Job{}, err
	}
	defer connection.Close()

	request, err := jobInputToGRPCRequest(input)
	if err != nil {
		return Job{}, err
	}

	response := &structpb.Struct{}
	if err := connection.Invoke(ctx, submitJobMethod, request, response); err != nil {
		return Job{}, err
	}

	fields := response.AsMap()
	return Job{
		ID:       int(fields["id"].(float64)),
		Type:     JobType(fields["type"].(string)),
		Status:   JobStatus(fields["status"].(string)),
		Priority: JobPriority(fields["priority"].(string)),
	}, nil
}

func grpcRequestToJobInput(request *structpb.Struct) (SubmitJobInput, error) {
	fields := request.AsMap()
	jobType, _ := fields["type"].(string)
	if jobType == "" {
		return SubmitJobInput{}, fmt.Errorf("type is required")
	}

	priority, _ := fields["priority"].(string)
	if priority != "" && priority != string(JobPriorityLow) && priority != string(JobPriorityMedium) && priority != string(JobPriorityHigh) {
		return SubmitJobInput{}, fmt.Errorf("priority must be low, medium, or high")
	}

	payload := map[string]string{}
	if rawPayload, ok := fields["payload"].(map[string]any); ok {
		for key, value := range rawPayload {
			payload[key] = fmt.Sprint(value)
		}
	}

	maxRetries := 0
	if value, ok := fields["max_retries"].(float64); ok {
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
		Type:        JobType(jobType),
		Payload:     payload,
		Priority:    JobPriority(priority),
		ScheduledAt: scheduledAt,
		MaxRetries:  maxRetries,
	}, nil
}

func jobInputToGRPCRequest(input SubmitJobInput) (*structpb.Struct, error) {
	payload := make(map[string]any, len(input.Payload))
	for key, value := range input.Payload {
		payload[key] = value
	}

	request := map[string]any{
		"type":        string(input.Type),
		"payload":     payload,
		"priority":    string(input.Priority),
		"max_retries": input.MaxRetries,
	}
	if input.ScheduledAt != nil {
		request["scheduled_at"] = input.ScheduledAt.Format(time.RFC3339)
	}

	return structpb.NewStruct(request)
}

func jobToGRPCResponse(job Job) (*structpb.Struct, error) {
	return structpb.NewStruct(map[string]any{
		"id":       job.ID,
		"type":     string(job.Type),
		"status":   string(job.Status),
		"priority": string(job.Priority),
	})
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
