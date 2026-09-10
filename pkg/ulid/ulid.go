package ulid

import (
	"crypto/rand"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

var (
	entropy *ulid.MonotonicEntropy
	once    sync.Once
)

func initEntropy() {
	once.Do(func() {
		entropy = ulid.Monotonic(rand.Reader, 0)
	})
}

func New() string {
	initEntropy()

	ms := ulid.Timestamp(time.Now())

	id, err := ulid.New(ms, entropy)
	if err != nil {
		return ulid.Make().String()
	}

	return id.String()
}

func IsValid(id string) bool {
	_, err := ulid.Parse(id)
	return err == nil
}
