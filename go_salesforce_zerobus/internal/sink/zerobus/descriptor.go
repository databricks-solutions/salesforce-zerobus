package zerobus

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/event"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/proto/eventpb"
)

// Descriptor returns the serialized DescriptorProto of SalesforceEvent for
// format f, which Zerobus needs for proto-mode ingestion. Fields the format
// omits are dropped, so the target table does not need their columns (rows
// built for f never set them).
func Descriptor(f event.Format) ([]byte, error) {
	fd := (&eventpb.SalesforceEvent{}).ProtoReflect().Descriptor().ParentFile()
	fdp := protodesc.ToFileDescriptorProto(fd)
	for _, msg := range fdp.MessageType {
		if msg.GetName() == "SalesforceEvent" {
			fields := msg.Field[:0]
			for _, field := range msg.Field {
				if f.Includes(field.GetName()) {
					fields = append(fields, field)
				}
			}
			msg.Field = fields
			return proto.Marshal(msg)
		}
	}
	return nil, fmt.Errorf("SalesforceEvent message not found in file descriptor")
}
