package client

import (
	"errors"
	"strconv"
	"strings"

	"go.mau.fi/whatsmeow"
)

// definiteRefusalCode recognizes only the pinned dependency's explicit server
// acknowledgements: a typed IQ error or fmt.Errorf("%w %d", sentinel, code).
// Walk wrappers to inspect the node directly wrapping the sentinel; arbitrary
// surrounding text, lookalike errors, and malformed codes are not delivery proof.
func definiteRefusalCode(err error) (int, bool) {
	var iq *whatsmeow.IQError
	if errors.As(err, &iq) && iq.Code > 0 {
		return iq.Code, true
	}
	for node := err; node != nil; node = errors.Unwrap(node) {
		if errors.Unwrap(node) != whatsmeow.ErrServerReturnedError {
			continue
		}
		prefix := whatsmeow.ErrServerReturnedError.Error() + " "
		if !strings.HasPrefix(node.Error(), prefix) {
			return 0, false
		}
		raw := strings.TrimPrefix(node.Error(), prefix)
		if len(raw) != 3 || raw[0] < '1' || raw[0] > '9' || raw[1] < '0' || raw[1] > '9' || raw[2] < '0' || raw[2] > '9' {
			return 0, false
		}
		code, err := strconv.Atoi(raw)
		return code, err == nil
	}
	return 0, false
}
