package jobs

import (
	"testing"

	"google.golang.org/protobuf/types/known/structpb"
)

func TestGRPCRequestToJobInput(t *testing.T) {
	request, err := structpb.NewStruct(map[string]any{
		"type":     "email",
		"priority": "high",
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
}
