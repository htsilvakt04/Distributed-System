package raft

import (
	"fmt"
	"log"
	"os"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.SetFlags(0)
		ms := time.Now().UnixNano()
		prefixed := fmt.Sprintf("%d %s", ms, fmt.Sprintf(format, a...))
		log.Print(prefixed)
		// Force flush to disk if writing to file
		if f, ok := log.Writer().(*os.File); ok {
			f.Sync()
		}
	}
}
