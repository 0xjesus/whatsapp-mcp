package client

import (
	"strings"
	"testing"

	waProto "go.mau.fi/whatsmeow/binary/proto"
	"google.golang.org/protobuf/proto"
)

func TestExtractTextContent_Nil(t *testing.T) {
	if got := extractTextContent(nil); got != "" {
		t.Fatalf("extractTextContent(nil) = %q, want empty string", got)
	}
}

func TestExtractTextContent_Conversation(t *testing.T) {
	msg := &waProto.Message{Conversation: proto.String("hi")}
	if got := extractTextContent(msg); got != "hi" {
		t.Fatalf("extractTextContent(Conversation=\"hi\") = %q, want \"hi\"", got)
	}
}

func TestExtractTextContent_ExtendedText(t *testing.T) {
	msg := &waProto.Message{
		ExtendedTextMessage: &waProto.ExtendedTextMessage{
			Text: proto.String("x"),
		},
	}
	if got := extractTextContent(msg); got != "x" {
		t.Fatalf("extractTextContent(ExtendedTextMessage.Text=\"x\") = %q, want \"x\"", got)
	}
}

func TestExtractTextContent_ConversationWinsOverExtendedText(t *testing.T) {
	// Conversation is checked first, so when both present the conversation wins.
	msg := &waProto.Message{
		Conversation: proto.String("primary"),
		ExtendedTextMessage: &waProto.ExtendedTextMessage{
			Text: proto.String("fallback"),
		},
	}
	if got := extractTextContent(msg); got != "primary" {
		t.Fatalf("extractTextContent = %q, want \"primary\"", got)
	}
}

func TestExtractMediaInfo_Nil(t *testing.T) {
	mediaType, filename, url, directPath, mediaKey, sha, encSHA, length := extractMediaInfo(nil)
	if mediaType != "" || filename != "" || url != "" || directPath != "" ||
		mediaKey != nil || sha != nil || encSHA != nil || length != 0 {
		t.Fatalf("extractMediaInfo(nil) should return zero values, got (%q,%q,%q,%q,%v,%v,%v,%d)",
			mediaType, filename, url, directPath, mediaKey, sha, encSHA, length)
	}
}

func TestExtractMediaInfo_Image(t *testing.T) {
	wantURL := "https://example.com/pic.jpg"
	wantDirectPath := "/v/t62.7118-24/abc.enc?ccb=11-4&oh=x&oe=y&_nc_sid=z"
	wantKey := []byte{0x01, 0x02}
	wantSHA := []byte{0x03, 0x04}
	wantEncSHA := []byte{0x05, 0x06}
	wantLen := uint64(1234)

	msg := &waProto.Message{
		ImageMessage: &waProto.ImageMessage{
			URL:           proto.String(wantURL),
			DirectPath:    proto.String(wantDirectPath),
			MediaKey:      wantKey,
			FileSHA256:    wantSHA,
			FileEncSHA256: wantEncSHA,
			FileLength:    proto.Uint64(wantLen),
		},
	}

	mediaType, filename, url, directPath, mediaKey, sha, encSHA, length := extractMediaInfo(msg)

	if mediaType != "image" {
		t.Errorf("mediaType = %q, want \"image\"", mediaType)
	}
	if !strings.HasPrefix(filename, "image_") || !strings.HasSuffix(filename, ".jpg") {
		t.Errorf("filename = %q, want pattern image_*.jpg", filename)
	}
	if url != wantURL {
		t.Errorf("url = %q, want %q", url, wantURL)
	}
	if directPath != wantDirectPath {
		t.Errorf("directPath = %q, want %q", directPath, wantDirectPath)
	}
	if string(mediaKey) != string(wantKey) {
		t.Errorf("mediaKey = %v, want %v", mediaKey, wantKey)
	}
	if string(sha) != string(wantSHA) {
		t.Errorf("fileSHA256 = %v, want %v", sha, wantSHA)
	}
	if string(encSHA) != string(wantEncSHA) {
		t.Errorf("fileEncSHA256 = %v, want %v", encSHA, wantEncSHA)
	}
	if length != wantLen {
		t.Errorf("fileLength = %d, want %d", length, wantLen)
	}
}

// TestPreferredDirectPath: whichever value the download layer hands to
// whatsmeow as DirectPath, it must prefer the stored protobuf DirectPath
// when one is present, and only fall back to string-parsing the URL for
// legacy rows written before the direct_path column existed.
func TestPreferredDirectPath(t *testing.T) {
	const storedCanonical = "/v/t62.7118-24/new.enc?ccb=11-4&oh=SIG&oe=EXP&_nc_sid=SID"
	const legacyURL = "https://mmg.whatsapp.net/v/t62.7118-24/legacy.enc?ccb=x"

	if got := preferredDirectPath(storedCanonical, legacyURL); got != storedCanonical {
		t.Errorf("with stored: got %q, want %q (stored wins)", got, storedCanonical)
	}

	// Empty stored → fall back to URL parse. Exact expected value here is
	// whatever extractDirectPathFromURL currently returns; verify by
	// comparing against a fresh call to keep this test tolerant of PR-#22
	// behaviour changes.
	want := extractDirectPathFromURL(legacyURL)
	if got := preferredDirectPath("", legacyURL); got != want {
		t.Errorf("empty stored: got %q, want extractDirectPathFromURL result %q", got, want)
	}
}

func TestExtractDirectPathFromURL(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			// WhatsApp's CDN URLs carry the signed auth params in the query
			// (ccb/oh/oe/_nc_sid). They MUST be preserved: whatsmeow builds
			// the download URL as `https://<host><directPath>&hash=…`, which
			// assumes directPath already ends in `?…`. Stripping the query
			// yields a malformed URL that the CDN answers with HTTP 403.
			name: "cdn url with query — query preserved for signed CDN auth",
			in:   "https://mmg.whatsapp.net/v/t62.7118-24/abc.enc?ccb=11-4&oh=01_Q5xx&oe=6A6220F0&_nc_sid=5e03e0&mms3=true",
			want: "/v/t62.7118-24/abc.enc?ccb=11-4&oh=01_Q5xx&oe=6A6220F0&_nc_sid=5e03e0&mms3=true",
		},
		{
			name: "cdn url without query",
			in:   "https://mmg.whatsapp.net/v/t62.7118-24/abc.enc",
			want: "/v/t62.7118-24/abc.enc",
		},
		{
			name: "not a url",
			in:   "not-a-url",
			want: "not-a-url",
		},
		{
			name: "empty",
			in:   "",
			want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := extractDirectPathFromURL(tc.in); got != tc.want {
				t.Fatalf("extractDirectPathFromURL(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestExtract_Sticker(t *testing.T) {
	mt, fname, _, _, _, _, _, _ := extractMediaInfo(&waProto.Message{StickerMessage: &waProto.StickerMessage{
		URL: proto.String("https://mmg/x"), DirectPath: proto.String("/v/x"), MediaKey: []byte{1}, FileSHA256: []byte{2}, FileEncSHA256: []byte{3}, FileLength: proto.Uint64(10), Mimetype: proto.String("image/webp")}})
	if mt != "sticker" || !strings.HasSuffix(fname, ".webp") {
		t.Fatalf("sticker = %q %q", mt, fname)
	}
	if text := extractTextContent(&waProto.Message{StickerMessage: &waProto.StickerMessage{IsAnimated: proto.Bool(true)}}); text != "[sticker animado]" {
		t.Fatalf("animated sticker text = %q", text)
	}
}

func TestExtract_Location(t *testing.T) {
	text := extractTextContent(&waProto.Message{LocationMessage: &waProto.LocationMessage{
		DegreesLatitude: proto.Float64(19.4326), DegreesLongitude: proto.Float64(-99.1332), Name: proto.String("Centro"), Address: proto.String("Centro")}})
	if text != "[ubicación] Centro · 19.4326,-99.1332" {
		t.Fatalf("location = %q", text)
	}
	text = extractTextContent(&waProto.Message{LocationMessage: &waProto.LocationMessage{DegreesLatitude: proto.Float64(1), DegreesLongitude: proto.Float64(2), Address: proto.String("Calle 1")}})
	if text != "[ubicación] Calle 1 · 1,2" {
		t.Fatalf("location by address = %q", text)
	}
	text = extractTextContent(&waProto.Message{LiveLocationMessage: &waProto.LiveLocationMessage{DegreesLatitude: proto.Float64(1.5), DegreesLongitude: proto.Float64(2.5), Caption: proto.String("voy")}})
	if text != "[ubicación en vivo] 1.5,2.5 · voy" {
		t.Fatalf("live location = %q", text)
	}
}

func TestExtract_Contacts(t *testing.T) {
	vcard := "BEGIN:VCARD\nVERSION:3.0\nFN:Ana Ejemplo\nTEL;type=CELL:+44 7700 900101\nTEL:+44 7700 900102\nEND:VCARD"
	text := extractTextContent(&waProto.Message{ContactMessage: &waProto.ContactMessage{DisplayName: proto.String("Ana Ejemplo"), Vcard: proto.String(vcard)}})
	if text != "[contacto] Ana Ejemplo · +44 7700 900101, +44 7700 900102" {
		t.Fatalf("contact = %q", text)
	}
	text = extractTextContent(&waProto.Message{ContactsArrayMessage: &waProto.ContactsArrayMessage{DisplayName: proto.String("2 contactos"),
		Contacts: []*waProto.ContactMessage{{DisplayName: proto.String("A")}, {DisplayName: proto.String("B")}}}})
	if text != "[contactos] 2: A, B" {
		t.Fatalf("contacts = %q", text)
	}
}

func TestUnsupportedKind(t *testing.T) {
	name, signal := unsupportedKind(&waProto.Message{OrderMessage: &waProto.OrderMessage{}})
	if name != "orderMessage" || signal {
		t.Fatalf("order = %q %v", name, signal)
	}
	name, signal = unsupportedKind(&waProto.Message{SenderKeyDistributionMessage: &waProto.SenderKeyDistributionMessage{}})
	if name != "senderKeyDistributionMessage" || !signal {
		t.Fatalf("skdm = %q %v", name, signal)
	}
	name, _ = unsupportedKind(&waProto.Message{MessageContextInfo: &waProto.MessageContextInfo{}})
	if name != "" {
		t.Fatalf("context-only message must report no kind, got %q", name)
	}
}

func TestHandleMessage_UnsupportedBecomesRow(t *testing.T) {
	c, s := mutationClient(t)
	c.handleMessage(mutationEvent(&waProto.Message{OrderMessage: &waProto.OrderMessage{}}))
	var content, mediaType string
	err := s.DB().QueryRow("SELECT content, media_type FROM messages WHERE id='protocol-envelope' AND chat_jid=?", mutationChat).Scan(&content, &mediaType)
	if err != nil || mediaType != "unsupported" || content != "[sin soporte: orderMessage]" {
		t.Fatalf("unsupported row = %q %q %v", content, mediaType, err)
	}
	c.handleMessage(mutationEvent(&waProto.Message{SenderKeyDistributionMessage: &waProto.SenderKeyDistributionMessage{}}))
	var n int
	_ = s.DB().QueryRow("SELECT count(*) FROM messages WHERE media_type='unsupported'").Scan(&n)
	if n != 1 {
		t.Fatalf("signal-only message stored as unsupported: %d rows", n)
	}
}
