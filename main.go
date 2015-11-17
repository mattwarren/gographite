package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net"
	"regexp"
	"sort"
	"strconv"
	"time"
)

const (
	TCP = "tcp"
	UDP = "udp"
)

type Packet struct {
	Bucket   string
	Value    int
	Modifier string
	Sampling float32
}

// This is just shorthand for defining multiple variables in one go, see https://www.golang-book.com/books/intro/4#section4
var (
	// flag.String(..) The return value is the address of a string variable that stores the value of the flag.
	serviceAddress   = flag.String("address", ":8125", "UDP service address")
	graphiteAddress  = flag.String("graphite", "localhost:2003", "Graphite service address")
	flushInterval    = flag.Int64("flush-interval", 10, "Flush interval")
	percentThreshold = flag.Int("percent-threshold", 90, "Threshold percent")
)

var (
	// Sends to a buffered channel, will block only when the buffer is full. Receives block when the buffer is empty.
	In       = make(chan Packet, 10000) // buffered channel with length 10,000
	counters = make(map[string]int)
	timers   = make(map[string][]int)
)

func monitor() {
	var err error
	if err != nil {
		log.Println(err)
	}
	timer := time.NewTicker(time.Duration(*flushInterval) * time.Second)
	for {
		select {
		case <-timer.C: // when we get a "Tick" on the Timer channel
			//log.Println("Timer fired")
			submit()
		case s := <-In: // when we get a packet in the In channel
			if s.Modifier == "ms" {
				_, ok := timers[s.Bucket]
				if !ok {
					var timer []int
					timers[s.Bucket] = timer
				}
				timers[s.Bucket] = append(timers[s.Bucket], s.Value)
			} else {
				_, ok := counters[s.Bucket]
				if !ok {
					counters[s.Bucket] = 0
				}
				counters[s.Bucket] += int(float32(s.Value) * (1 / s.Sampling))
			}
		}
	}
}

func submit() {
	client, err := net.Dial(TCP, *graphiteAddress)
	if client != nil {
		numStats := 0
		now := time.Now()
		buffer := bytes.NewBufferString("")
		for bucket, counter := range counters {
			value := int64(counter) / ((*flushInterval * int64(time.Second)) / 1e3)
			fmt.Fprintf(buffer, "stats.%s %d %d\n", bucket, value, now)
			fmt.Fprintf(buffer, "stats_counts.%s %d %d\n", bucket, counter, now)
			counters[bucket] = 0
			numStats++
		}
		for bucket, timer := range timers {
			if len(timer) > 0 {
				sort.Ints(timer)
				min := timer[0]
				max := timer[len(timer)-1]
				mean := min
				maxAtThreshold := max
				count := len(timer)
				if len(timer) > 1 {
					var thresholdIndex = ((100 - *percentThreshold) / 100) * count
					numInThreshold := count - thresholdIndex
					values := timer[0:numInThreshold]

					sum := 0
					for i := 0; i < numInThreshold; i++ {
						sum += values[i]
					}
					mean = sum / numInThreshold
				}
				// This is clearing out the item we've just read, by setting it to a new empty array
				var z []int
				timers[bucket] = z

				fmt.Fprintf(buffer, "stats.timers.%s.mean %d %d\n", bucket, mean, now)
				fmt.Fprintf(buffer, "stats.timers.%s.upper %d %d\n", bucket, max, now)
				fmt.Fprintf(buffer, "stats.timers.%s.upper_%d %d %d\n", bucket, *percentThreshold, maxAtThreshold, now)
				fmt.Fprintf(buffer, "stats.timers.%s.lower %d %d\n", bucket, min, now)
				fmt.Fprintf(buffer, "stats.timers.%s.count %d %d\n", bucket, count, now)
			}
			numStats++
		}
		fmt.Fprintf(buffer, "statsd.numStats %d %d\n", numStats, now)
		client.Write(buffer.Bytes())
		client.Close()
	} else {
		log.Printf(err.Error())
	}
}

func handleMessage(conn *net.UDPConn, remaddr net.Addr, buf *bytes.Buffer) {
	var packet Packet
	var sanitizeRegexp = regexp.MustCompile("[^a-zA-Z0-9\\-_\\.:\\|@]")
	var packetRegexp = regexp.MustCompile("([a-zA-Z0-9_]+):([0-9]+)\\|(c|ms)(\\|@([0-9\\.]+))?")
	s := sanitizeRegexp.ReplaceAllString(buf.String(), "")
	for _, item := range packetRegexp.FindAllStringSubmatch(s, -1) {
		value, err := strconv.Atoi(item[2])
		if err != nil {
			if item[3] == "ms" {
				value = 0
			} else {
				value = 1
			}
		}

		sampleRate, err := strconv.ParseFloat(item[5], 32)
		if err != nil {
			sampleRate = 1
		}

		packet.Bucket = item[1]
		packet.Value = value
		packet.Modifier = item[3]
		packet.Sampling = float32(sampleRate)
		In <- packet
	}
}

func udpListener() {
	address, _ := net.ResolveUDPAddr(UDP, *serviceAddress)
	listener, err := net.ListenUDP(UDP, address)
	if err != nil {
		log.Fatalf("ListenAndServe: %s", err.Error())
	}
	for {
		// "make" allocate an array and returns a slice that refers to that array
		message := make([]byte, 512)
		n, remaddr, error := listener.ReadFrom(message)
		if error != nil {
			continue
		}
		buf := bytes.NewBuffer(message[0:n])
		go handleMessage(listener, remaddr, buf)

		// Process the message asynchronously by firing off a go-routine
		// If successful they are posted to the "In" channel and picked up by the "monitor()" method
	}
	listener.Close()
}

func main() {
	flag.Parse()
	log.Printf("serviceAddress: \"%s\"", *serviceAddress)
	log.Printf("graphiteAddress: \"%s\"", *graphiteAddress)
	log.Printf("flushInterval: %d", *flushInterval)
	log.Printf("percentThreshold: %d", *percentThreshold)
	go udpListener()
	monitor()
}
