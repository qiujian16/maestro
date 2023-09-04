package manifests

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	cepbv2 "github.com/cloudevents/sdk-go/binding/format/protobuf/v2"
	cepb "github.com/cloudevents/sdk-go/binding/format/protobuf/v2/pb"
	cloudeventstypes "github.com/cloudevents/sdk-go/v2/types"
	"github.com/kube-orchestra/maestro/internal/db"
	v1 "github.com/kube-orchestra/maestro/proto/api/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	cetypes "open-cluster-management.io/api/cloudevents/generic/types"
	workpayload "open-cluster-management.io/api/cloudevents/work/payload"
)

type CloudEventsService struct {
	v1.UnimplementedCloudEventsServiceServer
	resourceChan chan<- db.Resource
}

func NewCloudEventsService(resourceChan chan<- db.Resource) *CloudEventsService {
	return &CloudEventsService{resourceChan: resourceChan}
}

func (svc *CloudEventsService) Send(_ context.Context, r *cepb.CloudEvent) (*v1.CloudEventSendResponse, error) {
	evt, err := cepbv2.FromProto(r)
	if err != nil {
		return nil, fmt.Errorf("failed to convert protobuf to cloudevent: %v", err)
	}

	eventType, err := cetypes.ParseCloudEventsType(evt.Type())
	if err != nil {
		return nil, fmt.Errorf("failed to parse cloud event type %s, %v", evt.Type(), err)
	}

	if eventType.CloudEventsDataType != workpayload.ManifestEventDataType {
		return nil, fmt.Errorf("unsupported cloudevents data type %s", eventType.CloudEventsDataType)
	}

	evtExtensions := evt.Context.GetExtensions()

	resourceID, err := cloudeventstypes.ToString(evtExtensions[cetypes.ExtensionResourceID])
	if err != nil {
		return nil, fmt.Errorf("failed to get resourceid extension: %v", err)
	}

	resourceVersion, err := cloudeventstypes.ToString(evtExtensions[cetypes.ExtensionResourceVersion])
	if err != nil {
		return nil, fmt.Errorf("failed to get resourceversion extension: %v", err)
	}

	resourceVersionInt, err := strconv.ParseInt(resourceVersion, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to convert resourceversion - %v to int64", resourceVersion)
	}

	clusterName, err := cloudeventstypes.ToString(evtExtensions[cetypes.ExtensionClusterName])
	if err != nil {
		return nil, fmt.Errorf("failed to get clustername extension: %v", err)
	}

	var unstructuredObject unstructured.Unstructured
	switch evt.Context.GetDataContentType() {
	case "application/json", "":
		manifest := &workpayload.Manifest{}
		err = json.Unmarshal(evt.Data(), manifest)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal event data as resource: %v", err)
		}
		unstructuredObject = manifest.Manifest
	case "application/protobuf":
		// TODO(morvencao): add protobuf content type support
		return nil, fmt.Errorf("protobuf data content type not supported")
	default:
		return nil, fmt.Errorf("unsupported data content type %s", evt.Context.GetDataContentType())
	}

	res := db.Resource{
		Id:                   resourceID,
		ConsumerId:           clusterName,
		Object:               unstructuredObject,
		ResourceGenerationID: resourceVersionInt,
	}

	// TODO: check that it doesn't exist
	err = db.PutResource(&res)
	if err != nil {
		return nil, err
	}

	svc.resourceChan <- res

	return &v1.CloudEventSendResponse{
		Message: "Manifest posted successfully.",
		Status:  v1.CloudEventSendResponse_OK,
	}, nil
}
