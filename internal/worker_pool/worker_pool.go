package workerpool

import "fmt"

// Job represents a job to be executed.
// Being an interface with only a Do() method allows the worker pool to be
// agnostic of the actual jobs
// Any struct with a Do() method can be Job.
type Job interface {
	Do()
}

// JobQueue represents that job requests for workers to consume and execute.
// Implemented as buffered channel.
// Clients submit Jobs by sending them in JobQueue.
// The Dispatcher reads from JobQueue, and sends each Job to the jobChannel of
// an available worker
type JobQueue chan Job

// worker represents a worker that executes Jobs.
type worker struct {
	// workerPool is used by the worker to declare it is available to receive jobs.
	// when available, the worker sends its jobChannel to workerPool.
	// The Dispacher reads from workerPool, gets the jobChannel of an available
	// worker and sends a Job to it.
	workerPool chan chan Job
	// jobChannel is used to submit Jobs to the worker.
	jobChannel chan Job
	quit       chan bool
}

// Dispatcher provides the worker pool API.
// It exports the JobQueue in which clients can submit Jobs.
type Dispatcher struct {
	JobQueue   JobQueue
	workerPool chan chan Job
	maxWorkers int
	workers    []worker
}

func newWorker(workerPool chan chan Job) worker {
	return worker{
		workerPool: workerPool,
		jobChannel: make(chan Job),
		quit:       make(chan bool)}
}

func (w worker) start() {
	go func() {
		for {
			// when starting, or done with the previous Job,
			// register yourself to the worker into the worker queue.
			w.workerPool <- w.jobChannel

			select {
			case job := <-w.jobChannel:
				job.Do()
			case <-w.quit:
				fmt.Println("done, exiting")
				return
			}
		}
	}()
}

// NewDispatcher creates and returns a new Dispatcher
func NewDispatcher(maxWorkers, maxQueue int) *Dispatcher {
	return &Dispatcher{
		JobQueue:   make(chan Job, maxQueue),
		workerPool: make(chan chan Job, maxWorkers),
		maxWorkers: maxWorkers,
		workers:    make([]worker, maxWorkers),
	}
}

// Run starts the works and the dispatch goroutine
func (d *Dispatcher) Run() {
	for i := 0; i < d.maxWorkers; i++ {
		d.workers[i] = newWorker(d.workerPool)
		d.workers[i].start()
	}

	go d.dispatch()
}

// Stop stops the dispatcher
func (d *Dispatcher) Stop() {
	for i := 0; i < d.maxWorkers; i++ {
		d.workers[i].quit <- true
	}
}

func (d *Dispatcher) dispatch() {
	for {
		select {
		case job := <-d.JobQueue:
			go func(job Job) {
				// Try to obtain a worker job channel that is available.
				// Blocks until a worker is available.
				jobChannel := <-d.workerPool

				jobChannel <- job
			}(job)
		}
	}
}
