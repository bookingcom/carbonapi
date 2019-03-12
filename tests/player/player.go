// Package player for pre-recorded TCP traffic for carbonapi
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"encoding/csv"

	"github.com/pkg/errors"
)

// Point represents a single metric sample sent via TCP
type Point struct {
	epoch int
	path  string
	val   float64
}

func readPoint(ss []string) (Point, error) {
	res := Point{0, "", 0.0}
	if len(ss) != 3 {
		return res, errors.New("recording record has wrong number of items")
	}

	var err error
	res.epoch, err = strconv.Atoi(ss[0])
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

func readRecording(filename string) ([]Point, error) {
	f, err := os.Open(filename)
	if err != nil {
		return make([]Point, 0), errors.Wrap(err, "error while opening the file")
	}
	defer f.Close() // only reading, so this is fine

	r := csv.NewReader(f)
	recs, err := r.ReadAll()
	if err != nil {
		return make([]Point, 0), errors.Wrap(err, "error while reading and parsing the CSV file")
	}

	points := make([]Point, 0)
	for _, rec := range recs {
		p, err := readPoint(rec)
		if err != nil {
			return points, errors.Wrap(err, "could not read point from recording")
		}
		points = append(points, p)
	}

	return points, nil
}

func play(rec []Point, host string, port int, rate int) error {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", host, port), time.Second*5)
	if err != nil {
		return errors.Wrap(err, "error while making TCP connection")
	}
	defer conn.Close()

	for i, r := range rec {
		fmt.Printf("\r%d / %d", i+1, len(rec))
		_, err = conn.Write([]byte(fmt.Sprintf("%s %f %d\n", r.path, r.val, r.epoch)))
		if err != nil {
			return errors.Wrap(err, "error writing to TCP connection")
		}
		time.Sleep(time.Microsecond * time.Duration(1e6/rate))
	}

	return nil
}

func main() {
	recFname := flag.String("in", "", "file name of the recording")
	host := flag.String("host", "127.0.0.1", "host to send TCP packets to")
	port := flag.Int("port", 0, "port to send the TCP packets to")
	rate := flag.Int("rate", 1, "QPS. Default 1, max 1000")
	// TODO (grzkv): To be utilized later
	workers := flag.Int("workers", 1, "Number of concurrent workers. Each worker has separate TCP connection. The points in the recording are split between workers. Max 10")
	flag.Parse()

	l := log.New(os.Stderr, "", 1)

	if recFname == nil || *recFname == "" {
		l.Println("Please supply recording file name")
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
	if rate == nil || *rate > 1000 {
		l.Println("Rate not supplied or too big")
		os.Exit(1)
	}
	if workers == nil || *workers > 10 {
		l.Println("Invlid number of workers. Note that max is 10.")
		os.Exit(1)
	}

	rec, err := readRecording(*recFname)
	if err != nil {
		l.Println(err)
		os.Exit(1)
	}
	fmt.Printf("Read %d points to play.\n", len(rec))

	fmt.Printf("Starting metrics transmission to %s:%d ...\n", *host, *port)
	err = play(rec, *host, *port, *rate)
	if err != nil {
		l.Println("error when sending TCP requests: ", err)
	}
}
