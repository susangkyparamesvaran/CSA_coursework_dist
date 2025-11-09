package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os"

	"uk.ac.bris.cs/gameoflife/gol"
)

// the broker will keep track of the multiple GOLWorkers
type Broker struct {
	workerAddresses []string
}

type section struct {
	start int
	end   int
}

// assign section helper function from before
// helper func to assign sections of image to workers based on no. of threads
func assignSections(height, workers int) []section {

	// we need to calculate the minimum number of rows for each worker
	minRows := height / workers
	// then say if we have extra rows left over then we need to assign those evenly to each worker
	extraRows := height % workers

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

// one iteration of the game using all workers
func (broker *Broker) ProcessTurn(req gol.BrokerRequest, res *gol.BrokerResponse) error {
	p := req.Params
	world := req.World

	numWorkers := len(broker.workerAddresses)

	// throw an error in teh case of there not being any workers dialled
	if numWorkers == 0 {
		return fmt.Errorf("no workers dialled")
	}

	// assign different sections of the image to each worker (aws node)
	sections := assignSections(p.ImageHeight, numWorkers)

	type sectionResult struct {
		start int
		rows  [][]byte
		err   error
	}

	resultsChan := make(chan sectionResult, numWorkers)

	// for each worker, assign the sections
	for i, address := range broker.workerAddresses {
		i, address := i, address
		section := sections[i]

		// process for each worker
		go func() {

			client, err := rpc.Dial("tcp", address)
			if err != nil {
				resultsChan <- sectionResult{err: fmt.Errorf("dial %s: %w", address, err)}
				return
			}

			defer client.Close()

			// section request
			sectionReq := gol.SectionRequest{
				Params: p,
				World:  world,
				StartY: section.start,
				EndY:   section.end,
			}

			var sectionRes gol.SectionResponse
			if err := client.Call("GOLWorker.ProcessTurn", sectionReq, &sectionRes); err != nil {
				resultsChan <- sectionResult{err: fmt.Errorf("dial %s: %w", address, err)}
				return
			}

			resultsChan <- sectionResult{
				start: sectionRes.StartY,
				rows:  sectionRes.Section,
				err:   nil,
			}

		}()
	}

	results := make([]sectionResult, numWorkers)
	for i := 0; i < numWorkers; i++ {
		results[i] = <-resultsChan
	}

	close(resultsChan)

	// build new world from the individual sections
	newWorld := make([][]byte, p.ImageHeight)
	for _, result := range results {
		for i, row := range result.rows {
			newWorld[result.start+i] = row
		}
	}

	res.World = newWorld
	return nil
}

func main() {

	broker := &Broker{
		workerAddresses: []string{
			"54.209.189.255:8030",
		},
	}

	err := rpc.RegisterName("Broker", broker)

	if err != nil {
		fmt.Println("Error registering RPC:", err)
		os.Exit(1)
		return
	}

	listener, err := net.Listen("tcp4", "0.0.0.0:8040")
	if err != nil {
		fmt.Println("Error starting listener:", err)
		os.Exit(1)
		return
	}
	fmt.Println("Worker listening on port 8040 (IPv4)...")

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
