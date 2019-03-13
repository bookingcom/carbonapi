// Package player for pre-recorded TCP traffic for carbonapi
package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"time"

	"encoding/csv"

	"github.com/pkg/errors"
)

// Point represents a single metric sample sent via TCP
type Point struct {
	epoch int32
	path  string
	val   float64
}

func readPoint(ss []string) (Point, error) {
	res := Point{0, "", 0.0}
	if len(ss) != 3 {
		return res, errors.New("recording record has wrong number of items")
	}

	var err error
	var ep int64
	ep, err = strconv.ParseInt(ss[0], 10, 32)
	res.epoch = int32(ep)
	if err != nil {
		return res, errors.Wrap(err, "error while converting epoch")
	}
	res.path = ss[1]
	res.val, err = strconv.ParseFloat(ss[2], 64)
	if err != nil {
		return res, errors.Wrap(err, "error while parsing metric value")
	}

	return res, nil
}

func readRecording(filename string) ([]Point, int32, error) {
	f, err := os.Open(filename)
	if err != nil {
		return make([]Point, 0), 0, errors.Wrap(err, "error while opening the file")
	}
	defer f.Close() // only reading, so this is fine

	r := csv.NewReader(f)
	recs, err := r.ReadAll()
	if err != nil {
		return make([]Point, 0), 0, errors.Wrap(err, "error while reading and parsing the CSV file")
	}

	if len(recs) == 0 {
		return make([]Point, 0), 0, errors.New("empty recording file")
	}

	points := make([]Point, 0)
	minTime := int32(math.MaxInt32)
	maxTime := int32(0)
	for _, rec := range recs {
		p, err := readPoint(rec)
		if p.epoch < minTime {
			minTime = p.epoch
		}
		if p.epoch > maxTime {
			maxTime = p.epoch
		}
		if err != nil {
			return points, 0, errors.Wrap(err, "could not read point from recording")
		}
		points = append(points, p)
	}

	return points, maxTime - minTime, nil
}

func play(rec []Point, cycles int, duration int32, conn net.Conn, rate int, every int, shift int) error {
	for c := 0; c < cycles; c++ {
		for i, r := range rec {
			if i%every != shift {
				continue
			}
			fmt.Printf("\r%d / %d", i+1+len(rec)*c, len(rec)*cycles)
			_, err := conn.Write([]byte(fmt.Sprintf("%s %f %d\n", r.path, r.val, r.epoch+int32(c)*duration)))
			if err != nil {
				return errors.Wrap(err, "error writing to TCP connection")
			}

			if rate != 0 {
				time.Sleep(time.Microsecond * time.Duration(1e6/rate))
			}
		}
	}
	fmt.Println()

	return nil
}

func main() {
	recFname := flag.String("in", "", "file name of the recording")
	cycles := flag.Int("cycles", 1, "how many times to re-play the recording")
	host := flag.String("host", "127.0.0.1", "host to send TCP packets to")
	port := flag.Int("port", 0, "port to send the TCP packets to")
	rate := flag.Int("rate", 0, "QPS. 0 for max speed. Default 0, max val 10000")
	workers := flag.Int("workers", 1, "Number of concurrent workers. Each worker has separate TCP connection. The points in the recording are split between workers. Max 10")
	flag.Parse()

	l := log.New(os.Stderr, "", 1)

	if recFname == nil || *recFname == "" {
		l.Println("Please supply recording file name")
		os.Exit(1)
	}
	if cycles == nil {
		l.Println("Could not get number of cycles")
		os.Exit(1)
	}
	if host == nil {
		l.Println("Could not get host value")
		os.Exit(1)
	}
	if port == nil || *port == 0 {
		l.Println("Please supply port number")
		os.Exit(1)
	}
	if rate == nil || *rate > 10000 {
		l.Println("Rate not supplied or too big")
		os.Exit(1)
	}
	if workers == nil || *workers > 32 {
		l.Println("Invlid number of workers. Note that max is 32.")
		os.Exit(1)
	}

	rec, width, err := readRecording(*recFname)
	if err != nil {
		l.Println(err)
		os.Exit(1)
	}
	fmt.Printf("Read %d points to play in %d cycles.\n", len(rec), *cycles)

	fmt.Printf("Starting metrics transmission to %s:%d ...\n", *host, *port)

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", *host, *port), time.Second*5)
	if err != nil {
		l.Println("error while making TCP connection :", err)
		os.Exit(1)
	}
	defer conn.Close()

	eChan := make(chan error)
	for w := 0; w < *workers; w++ {
		go func(wNum int, err chan<- error) {
			pErr := play(rec, *cycles, width, conn, *rate, *workers, wNum)
			err <- pErr
		}(w, eChan)
	}

	for w := 0; w < *workers; w++ {
		err = <-eChan
		if err != nil {
			l.Println("error while sending data via TCP connection :", err)
		}
	}
}
