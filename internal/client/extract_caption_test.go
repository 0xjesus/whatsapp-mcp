package client

import (
	"testing"

	waProto "go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

func TestExtractTextContentReadsCaptions(t *testing.T) {
	img := &waProto.Message{ImageMessage: &waProto.ImageMessage{Caption: proto.String("photo text")}}
	if got := extractTextContent(img); got != "photo text" {
		t.Fatalf("image caption = %q", got)
	}
	vid := &waProto.Message{VideoMessage: &waProto.VideoMessage{Caption: proto.String("video text")}}
	if got := extractTextContent(vid); got != "video text" {
		t.Fatalf("video caption = %q", got)
	}
	child := &waProto.Message{AssociatedChildMessage: &waProto.FutureProofMessage{Message: img}}
	if got := extractTextContent(child); got != "photo text" {
		t.Fatalf("album child caption = %q", got)
	}
}
