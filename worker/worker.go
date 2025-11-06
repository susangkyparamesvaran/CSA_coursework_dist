package main

import (
	"fmt"
	"net"
	"net/rpc"

	"uk.ac.bris.cs/gameoflife/gol"
	"uk.ac.bris.cs/gameoflife/util"
)

type GOLWorker struct{}

type WorkerRequest struct {
	Params gol.Params
	World  [][]byte
}

type WorkerResponse struct {
	World [][]byte
	Alive []util.Cell
}

// structs for our channels used to communicate with the worker goroutine
type workerJob struct {
	startY int
	endY   int
	world  [][]byte
}

type workerResult struct {
	ID           int
	startY       int
	worldSection [][]byte
}

type section struct {
	start, end int
}

func (e *GOLWorker) ProcessTurns(req WorkerRequest, res *WorkerResponse) error {
	p := req.Params
	world := req.World

	// Channels to send work and receive results
	jobChan := make(chan workerJob)
	resultChan := make(chan workerResult)

	sections := assignSections(p.ImageHeight, p.Threads)

	// for each worker
	for i := 0; i < p.Threads; i++ {
		go worker(i, p, jobChan, resultChan)
	}

	//send one job per section
	for _, job := range sections {
		jobChan <- workerJob{
			startY: job.start,
			endY:   job.end,
			world:  world,
		}
	}
	close(jobChan)

	// collect all the resuts and put them into the new state of world
	results := make([]workerResult, 0, p.Threads)
	for i := 0; i < p.Threads; i++ {
		results = append(results, <-resultChan)
	}
	close(resultChan)

	// new world state
	newWorld := make([][]byte, p.ImageHeight)
	for i := range newWorld {
		newWorld[i] = make([]byte, p.ImageWidth)
	}

	for _, result := range results {
		start := result.startY
		for row := 0; row < len(result.worldSection); row++ {
			newWorld[start+row] = result.worldSection[row]
		}
	}

	// Populate response
	res.World = newWorld
	res.Alive = gol.AliveCells(newWorld, p.ImageWidth, p.ImageHeight)

	return nil
}

func calculateNextStates(p gol.Params, world [][]byte, startY, endY int) [][]byte {
	h := p.ImageHeight //h rows
	w := p.ImageWidth  //w columns

	rows := endY - startY

	//make new grid section
	newRows := make([][]byte, rows)
	for i := 0; i < rows; i++ {
		newRows[i] = make([]byte, w)
	}

	for i := startY; i < endY; i++ {
		for j := 0; j < w; j++ { //accessing each individual cell
			count := 0
			up := (i - 1 + h) % h
			down := (i + 1) % h
			left := (j - 1 + w) % w
			right := (j + 1) % w

			//need to check all it's neighbors and state of it's cell
			leftCell := world[i][left]
			if leftCell == 255 {
				count += 1
			}
			rightCell := world[i][right]
			if rightCell == 255 {
				count += 1
			}
			upCell := world[up][j]
			if upCell == 255 {
				count += 1
			}
			downCell := world[down][j]
			if downCell == 255 {
				count += 1
			}
			upRightCell := world[up][right]
			if upRightCell == 255 {
				count += 1
			}
			upLeftCell := world[up][left]
			if upLeftCell == 255 {
				count += 1
			}

			downRightCell := world[down][right]
			if downRightCell == 255 {
				count += 1
			}

			downLeftCell := world[down][left]
			if downLeftCell == 255 {
				count += 1
			}

			//update the cells
			if world[i][j] == 255 {
				if count == 2 || count == 3 {
					newRows[i-startY][j] = 255
				} else {
					newRows[i-startY][j] = 0
				}
			}

			if world[i][j] == 0 {
				if count == 3 {
					newRows[i-startY][j] = 255
				} else {
					newRows[i-startY][j] = 0
				}
			}

		}
	}
	return newRows
}

// worker goroutine
func worker(id int, p gol.Params, jobs <-chan workerJob, results chan<- workerResult) {
	for job := range jobs {
		outputSection := calculateNextStates(p, job.world, job.startY, job.endY)
		results <- workerResult{
			ID:           id,
			startY:       job.startY,
			worldSection: outputSection,
		}
	}
}

// helper func to assign sections of image to workers based on no. of threads
func assignSections(height, threads int) []section {
	// say if we had 16 rows and 4 threads
	// we want to be able to allocate say 4 rows to 1 thread, 4 to the other thread etc.
	workers := threads

	// we need to calculate the minimum number of rows for each worker
	minRows := height / threads
	// then say if we have extra rows left over then we need to assign those evenly to each worker
	extraRows := height % threads

	// make a slice, the size of the number of threads
	sections := make([]section, workers)
	start := 0

	for i := 0; i < workers; i++ {
		// assigns the base amount of rows to the thread
		rows := minRows
		// if say we're on worker 2 and there are 3 extra rows left,
		// then we can add 1 more job to the thread
		if i < extraRows {
			rows++
		}

		// marks where the end of the section ends
		end := start + rows
		// assigns these rows to the section
		sections[i] = section{start: start, end: end}
		// start is updated for the next worker
		start = end
	}
	return sections
}

func main() {
	err := rpc.Register(new(GOLWorker))
	if err != nil {
		fmt.Println("Error registering RPC:", err)
		return
	}

	listener, err := net.Listen("tcp4", "0.0.0.0:8030")
	if err != nil {
		fmt.Println("Error starting listener:", err)
		return
	}
	fmt.Println("Worker listening on port 8030 (IPv4)...")

	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Connection error:", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}
