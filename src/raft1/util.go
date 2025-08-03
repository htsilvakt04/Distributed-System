package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// Debugging
var Debug bool

func init() {
	debugEnv := os.Getenv("DEBUG_RAFT")
	Debug = strings.ToLower(debugEnv) == "true"
	if Debug {
		log.SetFlags(0)
	}
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		ms := time.Now().UnixNano()
		prefixed := fmt.Sprintf("%d %s", ms, fmt.Sprintf(format, a...))
		log.Print(prefixed)
	}
}
func LoadConfig() (Config, error) {
	var cfg Config

	// Get the filename of the current file (where this function is defined)
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return cfg, fmt.Errorf("unable to get caller info")
	}

	// Get the directory of the current file
	dir := filepath.Dir(filename)

	// Build full path to config.json located in the same directory
	configPath := filepath.Join(dir, "config.json")

	// Read the file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return cfg, err
	}

	// Unmarshal into the Config struct
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}

	return cfg, nil
}
