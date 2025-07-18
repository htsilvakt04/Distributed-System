package raft

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

// Debugging
var Debug bool

func init() {
	debugEnv := os.Getenv("DEBUG")
	Debug = strings.ToLower(debugEnv) == "true"
	if Debug {
		log.SetFlags(0)
	}
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		ms := time.Now().UnixMilli()
		prefixed := fmt.Sprintf("%d %s", ms, fmt.Sprintf(format, a...))
		log.Print(prefixed)
	}
}
