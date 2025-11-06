package gol

import (
	"fmt"
	"net/rpc"
	"time"

	"uk.ac.bris.cs/gameoflife/util"
)

type distributorChannels struct {
	events     chan<- Event
	ioCommand  chan<- ioCommand
	ioIdle     <-chan bool
	ioFilename chan<- string
	ioOutput   chan<- uint8
	ioInput    <-chan uint8
}

type WorkerRequest struct {
	Params Params
	World  [][]byte
}

type WorkerResponse struct {
	World [][]byte
	Alive []util.Cell
}

// distributor divides the work between workers and interacts with other goroutines.
func distributor(p Params, c distributorChannels, keypress <-chan rune) {

	// TODO: Create a 2D slice to store the world.

	filename := fmt.Sprintf("%dx%d", p.ImageWidth, p.ImageHeight)
	c.ioCommand <- ioInput
	c.ioFilename <- filename

	world := make([][]byte, p.ImageHeight)
	for y := 0; y < p.ImageHeight; y++ {
		world[y] = make([]byte, p.ImageWidth)
		for x := 0; x < p.ImageWidth; x++ {
			world[y][x] = <-c.ioInput
		}
	}

	// Start ticker to report alive cells every 2 seconds
	ticker := time.NewTicker(2 * time.Second)
	//Channel used to sognal the goroutine to stop
	done := make(chan bool)

	turn := 0

	go func() {
		for {
			select {
			// Case runs every time the timer ticks (every 2 seconds)
			case <-ticker.C:
				aliveCount := 0

				//loop to count alive cells
				for y := 0; y < p.ImageHeight; y++ {
					for x := 0; x < p.ImageWidth; x++ {
						if world[y][x] == 255 {
							aliveCount++
						}
					}
				}
				c.events <- AliveCellsCount{
					CompletedTurns: turn,
					CellsCount:     aliveCount,
				}
			case <-done:
				return
			}
		}
	}()

	// calculates which cells are alive in the inital state before a turn has been made
	initialAlive := AliveCells(world, p.ImageWidth, p.ImageHeight)
	if len(initialAlive) > 0 {
		c.events <- CellsFlipped{
			CompletedTurns: 0,
			Cells:          initialAlive}
	}

	c.events <- StateChange{turn, Executing}

	// for each turn it needs to split up the jobs,
	// such that there is one job from eahc section for each thread
	// needs to gather the results and then put them together for the newstate of world
	// TODO: Execute all turns of the Game of Life.

	//----------------------------------------------------------------------------------------------------------//
	//----------------------------------------------------------------------------------------------------------//

	// variables for step 5
	paused := false
	quitting := false

	client, err := rpc.Dial("tcp", "98.93.77.210:8030") // your AWS public IP + port
	if err != nil {
		fmt.Println("Error connecting to worker:", err)
		return
	}
	defer client.Close()

	for {
		select {
		case key := <-keypress:
			switch key {
			case 'p':
				if !paused {
					paused = true
					c.events <- StateChange{turn, Paused}
				} else {
					paused = false
					c.events <- StateChange{turn, Executing}
				}
			case 's':
				saveImage(p, c, world, turn)
			case 'q':
				quitting = true
			}
			continue
		default:
		}

		// quit if acc finished and not paused
		if quitting || (turn >= p.Turns && !paused) {
			break
		}

		if paused {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		request := WorkerRequest{
			Params: p,
			World:  world,
		}

		var response WorkerResponse

		err = client.Call("GOLWorker.ProcessTurns", request, &response)
		if err != nil {
			fmt.Println("Error calling remote worker:", err)
			quitting = true
			continue
		}

		///// STEP 6 CELLS FLIPPED///////////
		// At the end of each turn, put all changed coordinates into a slice,
		// and then send CellsFlipped event
		// make a slice so as to compare the old row and the new row of the world
		flippedCells := make([]util.Cell, 0)
		// go row by row, then column by column
		for y := 0; y < p.ImageHeight; y++ {
			for x := 0; x < p.ImageWidth; x++ {
				if world[y][x] != response.World[y][x] {
					flippedCells = append(flippedCells, util.Cell{X: x, Y: y})
				}
			}
		}

		// if there is at least one cell thats been flipped then we need to return the
		// Cells Flipped event
		if len(flippedCells) > 0 {
			c.events <- CellsFlipped{
				CompletedTurns: turn + 1,
				Cells:          flippedCells}
		}

		world = response.World

		///// STEP 6 TURN COMPLETE///////////
		// At the end of each turn we need to signal that a turn is completed
		c.events <- TurnComplete{
			CompletedTurns: turn + 1,
		}

		turn++ //advance turn
	}

	//Stop ticker after finishing all turns
	done <- true
	ticker.Stop()

	// TODO: Report the final state using FinalTurnCompleteEvent.
	aliveCells := AliveCells(world, p.ImageWidth, p.ImageHeight)
	c.events <- FinalTurnComplete{CompletedTurns: turn, Alive: aliveCells}

	//save final output
	saveImage(p, c, world, turn)
	c.events <- StateChange{turn, Quitting}

	//Close the channels to stop the SDL goroutine gracefully. Removing may cause deadlock.
	close(c.events)
	return

}

func AliveCells(world [][]byte, width, height int) []util.Cell {
	cells := make([]util.Cell, 0)
	for dy := 0; dy < height; dy++ {
		for dx := 0; dx < width; dx++ {
			if world[dy][dx] == 255 {
				cells = append(cells, util.Cell{X: dx, Y: dy})
			}
		}
	}
	return cells
}

// helper function to handle image saves
func saveImage(p Params, c distributorChannels, world [][]byte, turn int) {
	// Write final world to output file (PGM)
	// Construct the output filename in the required format
	// Example: "512x512x100" for a 512x512 world after 100 turns
	outFileName := fmt.Sprintf("%dx%dx%d", p.ImageWidth, p.ImageHeight, turn)
	c.ioCommand <- ioOutput     // telling the i/o goroutine that we are starting an output operation
	c.ioFilename <- outFileName // sending the filename to io goroutine

	for y := 0; y < p.ImageHeight; y++ {
		for x := 0; x < p.ImageWidth; x++ {
			//writing the pixel value to the ioOutput channel
			c.ioOutput <- world[y][x] //grayscale value for that pixel (0 or 255)
		}
	}

	// Make sure that the Io has finished any output before exiting.
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle

	// once saved, notify the SDL event system (important for Step 5)
	c.events <- ImageOutputComplete{CompletedTurns: turn, Filename: outFileName}

}
