package gw

const (
	// MAX_CONTROL_LINE_SIZE is the maximum allowed protocol control line size.
	// 1k should be plenty since payloads sans connect string are separate
	MAX_CONTROL_LINE_SIZE = 1024

	// PROTO_SNIPPET_SIZE is the default size of proto to print on parse errors.
	PROTO_SNIPPET_SIZE = 32

	// CR_LF string
	CR_LF = "\r\n"

	// LEN_CR_LF hold onto the computed size.
	LEN_CR_LF = len(CR_LF)

	BUFFER_SIZE = 1024
)
