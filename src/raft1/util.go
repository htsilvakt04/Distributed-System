package raft

import (
	"fmt"
	"log"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.SetFlags(0)
		ms := time.Now().UnixMilli()
		prefixed := fmt.Sprintf("%d %s", ms, fmt.Sprintf(format, a...))
		log.Print(prefixed)
	}
}
