package rsm

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	timestampBits = 41
	machineIDBits = 10
	sequenceBits  = 12

	maxMachineID = -1 ^ (-1 << machineIDBits)
	maxSequence  = -1 ^ (-1 << sequenceBits)

	timeShift      = sequenceBits + machineIDBits
	machineIDShift = sequenceBits

	epoch = int64(1609459200000) // Custom epoch: Jan 1, 2021
)

type Snowflake struct {
	mu        sync.Mutex
	lastTs    int64
	sequence  int64
	machineID int64
}

func NewSnowflake(machineID int64) (*Snowflake, error) {
	if machineID < 0 || machineID > maxMachineID {
		return nil, fmt.Errorf("machine ID must be between 0 and %d", maxMachineID)
	}
	return &Snowflake{
		machineID: machineID,
	}, nil
}

func currentMillis() int64 {
	return time.Now().UnixNano() / 1e6
}

func (s *Snowflake) NextID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	ts := currentMillis()
	if ts == s.lastTs {
		s.sequence = (s.sequence + 1) & maxSequence
		if s.sequence == 0 {
			// wait for the next millisecond
			for ts <= s.lastTs {
				ts = currentMillis()
			}
		}
	} else {
		s.sequence = 0
	}
	s.lastTs = ts

	id := ((ts - epoch) << timeShift) |
		(s.machineID << machineIDShift) |
		(s.sequence)
	return id
}

// Debugging
var Debug bool

func init() {
	debugEnv := os.Getenv("DEBUG_RSM")
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
