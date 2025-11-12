package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os"

	"uk.ac.bris.cs/gameoflife/gol"
)

type GOLWorker struct {
}

// process the section the broker gives
func (e *GOLWorker) ProcessSection(req gol.SectionRequest, res *gol.SectionResponse) error {
	p := req.Params
	world := req.World
	startY := req.StartY
	endY := req.EndY

	// call calculate state function on the requested section
	updatedSection := calculateNextStates(p, world, startY, endY)

	// update response to give back to broker
	res.StartY = startY
	res.Section = updatedSection
	return nil
}

// helper func to make worker shut down on keypress
func (e *GOLWorker) Shutdown(_ struct{}, _ *struct{}) error {
	fmt.Println("shutdown signal recieved, stopping worker.")

	go func() {
		os.Exit(0)
	}()
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

func main() {

	err := rpc.RegisterName("GOLWorker", new(GOLWorker))

	if err != nil {
		fmt.Println("Error registering RPC:", err)
		return
	}

	listener, err := net.Listen("tcp4", "0.0.0.0:8030")
	if err != nil {
		fmt.Println("Error starting listener:", err)
		os.Exit(1)
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
