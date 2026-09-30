package zerobus

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/proto/eventpb"
)

// Descriptor returns the serialized DescriptorProto of SalesforceEvent,
// which Zerobus needs for proto-mode ingestion.
func Descriptor() ([]byte, error) {
	fd := (&eventpb.SalesforceEvent{}).ProtoReflect().Descriptor().ParentFile()
	fdp := protodesc.ToFileDescriptorProto(fd)
	for _, msg := range fdp.MessageType {
		if msg.GetName() == "SalesforceEvent" {
			return proto.Marshal(msg)
		}
	}
	return nil, fmt.Errorf("SalesforceEvent message not found in file descriptor")
}
