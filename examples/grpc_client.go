package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"

	cepbv2 "github.com/cloudevents/sdk-go/binding/format/protobuf/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	v1 "github.com/kube-orchestra/maestro/proto/api/v1"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ceFilePath := flag.String("f", "", "path to the clouevents json file.")
	flag.Parse()

	ceJSONBytes, err := os.ReadFile(*ceFilePath)
	if err != nil {
		log.Fatal(err)
	}

	evt := &event.Event{}
	err = json.Unmarshal(ceJSONBytes, evt)
	if err != nil {
		log.Fatal(err)
	}

	pbEvt, err := cepbv2.ToProto(evt)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := grpc.Dial("localhost:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	cl := v1.NewCloudEventsServiceClient(conn)
	cl.Send(context.Background(), pbEvt)
}
