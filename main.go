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

var (
	serviceAddress  = flag.String("address", ":8125", "UDP service address")
	graphiteAddress = flag.String("graphie", "localhost:2003",
				"Graphite service address")
	flushInterval    = flag.Int64("flush-interval", 10, "Flush interval")
	percentThreshold = flag.Int("percent-threshold", 90, "Threshold percent")
)

var (
	In       = make(chan Packet, 10000)
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
		case <-t.C:
			submit()
		case s := <-In:
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
		message := make([]byte, 512)
		n, remaddr, error := listener.ReadFrom(message)
		if error != nil {
			continue
		}
		buf := bytes.NewBuffer(message[0:n])
		go handleMessage(listener, remaddr, buf)
	}
	listener.Close()
}

func main() {
	flag.Parse()
	go udpListener()
	monitor()
}
