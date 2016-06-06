package worker

import (
	"fmt"
	"testing"
	"time"
)

//func dummyHandler(key string, args interface{}) {
func dummyHandler(work *Work) {
	fmt.Printf("Dummy Handler \t")
	fmt.Printf("Work :: %v : %v\n", work.Key, work.Args)
}

func TestWorkerCanCallHandler(t *testing.T) {
	w := NewWorker(4, dummyHandler)

	dumbWork := &Work{Key: "hi",
		Args: "there",
	}

	w.Handler(dumbWork)

}

func TestWorker(t *testing.T) {

	w := NewWorker(4, dummyHandler)

	w.StartWork()

	dumbWork := &Work{Key: "hi",
		Args: "there",
	}

	w.Push(dumbWork)
	w.Push(dumbWork)

	anotherDumbWork := &Work{Key: "NOOO",
		Args: "there",
	}

	w.Push(anotherDumbWork)
	w.Push(anotherDumbWork)

	time.Sleep(6000 * time.Millisecond)

	// Should print 4 times

	w.StopWork()
}
