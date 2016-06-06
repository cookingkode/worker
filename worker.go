package worker

import (
	_ "fmt"
	log "github.com/Sirupsen/logrus"
	"hash/fnv"
)

type Work struct {
	Key  string
	Args interface{}
}

type Worker struct {
	channels [](chan *Work)
	quit     chan bool
	Lanes    uint
	Handler  func(work *Work)
}

func NewWorker(lanes uint, handler func(work *Work)) *Worker {
	wk := &Worker{
		Lanes:   lanes,
		Handler: handler,
	}

	wk.channels = make([](chan *Work), lanes)
	wk.quit = make(chan bool)

	var i uint
	for i = 0; i < lanes; i++ {
		wk.channels[i] = make(chan *Work, 50)
	}

	return wk
}

func (w *Worker) StartWork() {
	var i uint
	for i = 0; i < w.Lanes; i++ {
		log.Debug("[worker] Starting worker lane :", i)
		go w.wrapHandler(w.channels[i], i)
	}
}

func (w *Worker) StopWork() {
	w.quit <- true
}

func (w *Worker) Push(work *Work) {
	lane := getBucket(work.Key, w.Lanes)
	w.channels[lane] <- work
}

func (w *Worker) wrapHandler(c chan *Work, lane uint) {

	for {
		select {
		case <-w.quit:
			return
		case work := <-c:
			log.Debug("[worker] Got Work Starting Worker Lane ", lane)
			//fmt.Printf("[worker] Got Work Starting Worker Lane  %v\n", lane)
			w.Handler(work)
		}
	}
}

func getBucket(s string, nBuckets uint) uint {
	h := fnv.New32a()
	h.Write([]byte(s))
	return (uint(h.Sum32()) % nBuckets)
}
