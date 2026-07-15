package jobs

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestGRPCRequestToJobInput(t *testing.T) {
	request, err := structpb.NewStruct(map[string]any{
		"idempotency_key": "request-1",
		"type":            "email",
		"priority":        "high",
		"payload": map[string]any{
			"to": "user@example.com",
		},
		"max_retries": 2,
	})
	if err != nil {
		t.Fatal(err)
	}

	input, err := grpcRequestToJobInput(request)
	if err != nil {
		t.Fatal(err)
	}

	if input.Type != JobType("email") || input.Priority != JobPriorityHigh {
		t.Fatalf("unexpected gRPC job input: %+v", input)
	}

	if input.Payload["to"] != "user@example.com" || input.MaxRetries != 2 {
		t.Fatalf("unexpected gRPC payload: %+v", input)
	}
	if input.IdempotencyKey != "request-1" {
		t.Fatalf("unexpected idempotency key: %s", input.IdempotencyKey)
	}
}

func TestAuthenticateGRPC(t *testing.T) {
	interceptor := authenticateGRPC("secret-token")
	handler := func(ctx context.Context, request any) (any, error) {
		return "accepted", nil
	}

	validContext := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer secret-token"))
	response, err := interceptor(validContext, nil, &grpc.UnaryServerInfo{}, handler)
	if err != nil || response != "accepted" {
		t.Fatalf("expected valid token to be accepted: %v", err)
	}

	invalidContext := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer wrong-token"))
	_, err = interceptor(invalidContext, nil, &grpc.UnaryServerInfo{}, handler)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated error, got %v", err)
	}
}
