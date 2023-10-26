package marshaler

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	cepbv2 "github.com/cloudevents/sdk-go/binding/format/protobuf/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	v1 "github.com/kube-orchestra/maestro/proto/api/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

type CloudEventJSON struct {
}

func (c *CloudEventJSON) Marshal(v interface{}) ([]byte, error) {
	pbEvtSentResp, ok := v.(*v1.CloudEventSendResponse)
	if !ok {
		return nil, fmt.Errorf("v must be a CloudEventSendResponse")
	}

	return protojson.Marshal(pbEvtSentResp)
}

func (c *CloudEventJSON) Unmarshal(data []byte, v interface{}) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("v must be a non-nil pointer")
	}

	evt := &event.Event{}
	err := json.Unmarshal(data, evt)
	if err != nil {
		return err
	}
	pvEvent, err := cepbv2.ToProto(evt)
	if err != nil {
		return err
	}

	// set value of rv to value of pbEvent
	rv.Elem().Set(reflect.ValueOf(pvEvent).Elem())

	return nil
}

func (c *CloudEventJSON) NewDecoder(r io.Reader) runtime.Decoder {
	return DecoderWrapper{
		Decoder: json.NewDecoder(r),
	}
}

func (c *CloudEventJSON) NewEncoder(w io.Writer) runtime.Encoder {
	return EncoderWarpper{
		w: w,
	}
}

func (c *CloudEventJSON) ContentType(v interface{}) string {
	return "application/x-cloudevents"
}

type DecoderWrapper struct {
	*json.Decoder
}

func (d DecoderWrapper) Decode(v interface{}) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("v must be a non-nil pointer")
	}

	evt := &event.Event{}
	if err := d.Decoder.Decode(evt); err != nil {
		return err
	}

	pvEvent, err := cepbv2.ToProto(evt)
	if err != nil {
		return err
	}

	// set value of rv to value of pbEvent
	rv.Elem().Set(reflect.ValueOf(pvEvent).Elem())

	return nil
}

type EncoderWarpper struct {
	w io.Writer
}

func (e EncoderWarpper) Encode(v interface{}) error {
	pbEvtSentResp, ok := v.(*v1.CloudEventSendResponse)
	if !ok {
		return fmt.Errorf("v must be a CloudEventSendResponse")
	}

	bytes, err := protojson.Marshal(pbEvtSentResp)
	if err != nil {
		return err
	}

	_, err = e.w.Write(bytes)
	return err
}
