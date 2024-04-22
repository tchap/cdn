package app

import "strings"

const RemoteAddrSegmentReplacement = "X"

type LogRecordAnonymizer struct{}

func (t LogRecordAnonymizer) TransformRecord(r *LogRecord) *LogRecord {
	if i := strings.LastIndex(r.RemoteAddr, "."); i != -1 {
		r.RemoteAddr = r.RemoteAddr[:i+1] + RemoteAddrSegmentReplacement
	}
	return r
}
