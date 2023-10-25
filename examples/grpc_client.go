package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"

	cepbv2 "github.com/cloudevents/sdk-go/binding/format/protobuf/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	cloudeventstypes "github.com/cloudevents/sdk-go/v2/types"
	v1 "github.com/kube-orchestra/maestro/proto/api/v1"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	cetypes "open-cluster-management.io/api/cloudevents/generic/types"
)

func main() {
	ceFilePath := flag.String("f", "", "path to the clouevents json file.")
	flag.Parse()

	ceJSONBytes, err := os.ReadFile(*ceFilePath)
	if err != nil {
		log.Fatalf("failed to read file %s: %v", *ceFilePath, err)
	}

	evt := &event.Event{}
	err = json.Unmarshal(ceJSONBytes, evt)
	if err != nil {
		log.Fatalf("failed to unmarshal cloudevent: %v", err)
	}

	pbEvt, err := cepbv2.ToProto(evt)
	if err != nil {
		log.Fatalf("failed to convert cloudevent to protobuf: %v", err)
	}

	conn, err := grpc.Dial("localhost:31320", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	cl := v1.NewCloudEventsServiceClient(conn)
	cl.Send(context.Background(), pbEvt)

	evtExtensions := evt.Context.GetExtensions()
	resourceID, err := cloudeventstypes.ToString(evtExtensions[cetypes.ExtensionResourceID])
	if err != nil {
		log.Fatalf("failed to get resourceid extension: %v", err)
	}

	watchClient, err := cl.Watch(context.Background(), &v1.ResourceWatchRequest{Id: resourceID})
	if err != nil {
		log.Fatalf("failed to watch resource: %v", err)
	}

	for {
		newPbEvt, err := watchClient.Recv()
		if err != nil {
			log.Fatalf("failed to receive event: %v", err)
		}

		newEvt, err := cepbv2.FromProto(newPbEvt)
		if err != nil {
			log.Fatalf("failed to convert protobuf to cloudevent: %v", err)
		}

		newEvtJSON, err := json.Marshal(newEvt)
		if err != nil {
			log.Fatalf("failed to marshal cloudevent: %v", err)
		}

		log.Printf("Received event:\n%s\n\n", newEvtJSON)
	}
}
