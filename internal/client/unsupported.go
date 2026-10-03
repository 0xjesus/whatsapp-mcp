package client

import (
	"strconv"
	"strings"

	waProto "go.mau.fi/whatsmeow/binary/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Wrapper fields: present on many messages, never content by themselves.
var wrapperFields = map[string]bool{
	"messageContextInfo": true, "deviceSentMessage": true, "ephemeralMessage": true,
	"viewOnceMessage": true, "viewOnceMessageV2": true, "viewOnceMessageV2Extension": true,
	"documentWithCaptionMessage": true, "editedMessage": true,
}

// Signal-only kinds: logged at debug level, never stored as rows.
var signalOnlyFields = map[string]bool{
	"protocolMessage": true, "senderKeyDistributionMessage": true, "pollUpdateMessage": true,
	"reactionMessage": true, "keepInChatMessage": true, "encReactionMessage": true,
	"encCommentMessage": true, "encEventResponseMessage": true, "placeholderMessage": true,
}

// unsupportedKind names the first populated content field of msg that the daemon does not store,
// and whether it is signal-only. An empty name means nothing but wrapper fields was set.
func unsupportedKind(msg *waProto.Message) (string, bool) {
	if msg == nil {
		return "", false
	}
	name := ""
	msg.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		json := fd.JSONName()
		if wrapperFields[json] {
			return true
		}
		name = json
		return false
	})
	if name == "" {
		return "", false
	}
	return name, signalOnlyFields[name]
}

func formatCoords(lat, lng float64) string {
	return strconv.FormatFloat(lat, 'f', -1, 64) + "," + strconv.FormatFloat(lng, 'f', -1, 64)
}

// contactSummary: display name plus every TEL line of the vCard, in order.
func contactSummary(name, vcard string) string {
	var phones []string
	for _, line := range strings.Split(vcard, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(strings.ToUpper(line), "TEL") {
			if i := strings.LastIndex(line, ":"); i >= 0 && i+1 < len(line) {
				phones = append(phones, strings.TrimSpace(line[i+1:]))
			}
		}
	}
	if len(phones) == 0 {
		return name
	}
	return name + " · " + strings.Join(phones, ", ")
}
