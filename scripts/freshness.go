package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

var opType = "freshness"
var limit = 5

type durations []time.Duration

func (d durations) Len() int           { return len(d) }
func (d durations) Less(i, j int) bool { return int64(d[i]) < int64(d[j]) }
func (d durations) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }

func (d durations) percentile(p float64) time.Duration {
	return d[int(float64(d.Len())*p+0.5)-1]
}

type timelineRecord struct {
	recordWritten int64
	recordsRead   []int64
	ts            time.Time
	opType        operType
}

type timeLine []timelineRecord

func (t timeLine) Len() int           { return len(t) }
func (t timeLine) Less(i, j int) bool { return t[i].ts.Before(t[j].ts) }
func (t timeLine) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }

type operType int

const (
	vote         operType = iota
	mvUpdateVote operType = iota
	getHomepage  operType = iota
)

func buildOpLog(writeLog, queryLog [][]string) ([]timelineRecord, error) {
	opLog := make([]timelineRecord, 2*len(writeLog)+len(queryLog))

	var ts time.Time
	var err error
	var recID int64

	i := 0
	for _, fields := range writeLog {
		recID, err = strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			return opLog, err
		}

		ts, err = time.Parse("2006-01-02 15:04:05.000000", fields[1])
		if err != nil {
			return opLog, err
		}

		opLog[i] = timelineRecord{recordWritten: recID, ts: ts, opType: vote}
		i++

		ts, err = time.Parse("2006-01-02 15:04:05.000000", fields[2])
		if err != nil {
			return opLog, err
		}
		opLog[i] = timelineRecord{recordWritten: recID, ts: ts, opType: mvUpdateVote}
		i++
	}

	for _, fields := range queryLog {
		recIDs := strings.Split(fields[0], "|")
		rec := timelineRecord{
			recordsRead: make([]int64, len(recIDs)),
		}
		for j, id := range recIDs {
			recID, err = strconv.ParseInt(id, 10, 64)
			if err != nil {
				return opLog, err
			}
			rec.recordsRead[j] = recID
		}

		ts, err = time.Parse("2006-01-02 15:04:05.000000", fields[1])
		if err != nil {
			return opLog, err
		}

		rec.ts = ts
		rec.opType = getHomepage

		opLog[i] = rec
		i++
	}

	sort.Sort(timeLine(opLog))

	return opLog, nil
}

func postMortem(queryLog [][]string, opLog []timelineRecord) []int {
	stateDatastore := make(map[int64]int)
	stateQPU := make(map[int64]int)
	stalenessLog := make([]int, len(queryLog)*limit)

	i := 0
	for _, rec := range opLog {
		switch rec.opType {
		case vote:
			_, ok := stateDatastore[rec.recordWritten]
			if ok {
				stateDatastore[rec.recordWritten]++
			} else {
				stateDatastore[rec.recordWritten] = 1
			}
		case mvUpdateVote:
			_, ok := stateQPU[rec.recordWritten]
			if ok {
				stateQPU[rec.recordWritten]++
			} else {
				stateQPU[rec.recordWritten] = 1
			}
		case getHomepage:
			for _, r := range rec.recordsRead {
				if _, ok := stateQPU[r]; ok {
					stalenessLog[i] = stateDatastore[r] - stateQPU[r]
				} else {
					stalenessLog[i] = 0
				}
				i++
			}
		}
	}

	return stalenessLog
}

func freshnessVersions(writeLog, queryLog [][]string) error {
	opLog, err := buildOpLog(writeLog, queryLog)
	if err != nil {
		return err
	}

	stalenessLog := postMortem(queryLog, opLog)

	stalenessMap := make(map[int]int)
	for _, stalenessVal := range stalenessLog {
		_, found := stalenessMap[stalenessVal]
		if found {
			stalenessMap[stalenessVal]++
		} else {
			stalenessMap[stalenessVal] = 1
		}
	}

	freshnessVersions0 := float64(stalenessMap[0]) / float64((len(queryLog) * limit))

	sum := 0
	if v, ok := stalenessMap[1]; ok {
		sum = v
	}
	freshnessVersions1 := float64(sum) / float64((len(queryLog) * limit))

	sum = 0
	if v, ok := stalenessMap[2]; ok {
		sum = v
	}
	freshnessVersions2 := float64(sum) / float64((len(queryLog) * limit))

	sum = 0
	for _, i := range []int{3, 4, 5} {
		if v, ok := stalenessMap[i]; ok {
			sum += v
		}
	}
	freshnessVersions5 := float64(sum) / float64((len(queryLog) * limit))

	sum = 0
	for k, v := range stalenessMap {
		if k > 5 {
			sum += v
		}
	}
	freshnessVersions6 := float64(sum) / float64((len(queryLog) * limit))

	fmt.Printf("[%s-versions] =0: %.5f\n", opType, freshnessVersions0)
	fmt.Printf("[%s-versions] =1: %.5f\n", opType, freshnessVersions1)
	fmt.Printf("[%s-versions] =2: %.5f\n", opType, freshnessVersions2)
	fmt.Printf("[%s-versions] <=5: %.5f\n", opType, freshnessVersions5)
	fmt.Printf("[%s-versions] >5: %.5f\n", opType, freshnessVersions6)

	return nil
}

func freshnessLatency(writeLog [][]string) error {
	durationBuf := make([]time.Duration, 0)

	var tsDatastore, tsQPU time.Time
	var err error

	for _, fields := range writeLog {
		tsDatastore, err = time.Parse("2006-01-02 15:04:05.000000", fields[1])
		if err != nil {
			return err
		}

		tsQPU, err = time.Parse("2006-01-02 15:04:05.000000", fields[2])
		if err != nil {
			return err
		}

		durationBuf = append(durationBuf, tsQPU.Sub(tsDatastore))
	}

	freshnessDurations := durations(durationBuf)
	sort.Sort(freshnessDurations)

	fmt.Printf("[%s-latency] p50(ms): %.5f\n", opType, durationToMillis(freshnessDurations[freshnessDurations.Len()/2]))
	fmt.Printf("[%s-latency] p90(ms): %.5f\n", opType, durationToMillis(freshnessDurations.percentile(0.9)))
	fmt.Printf("[%s-latency] p95(ms): %.5f\n", opType, durationToMillis(freshnessDurations.percentile(0.95)))
	fmt.Printf("[%s-latency] p99(ms): %.5f\n", opType, durationToMillis(freshnessDurations.percentile(0.99)))

	return nil
}

func readLog(filePath string) ([][]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	log := make([][]string, 0)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), ",")
		log = append(log, fields)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return log, nil
}

func main() {
	if len(os.Args) > 1 {
		writeLog, err := readLog(os.Args[1])
		if err != nil {
			fmt.Println(err)
		}

		if err := freshnessLatency(writeLog); err != nil {
			log.Fatal(err)
		}

		if len(os.Args) > 2 {
			queryLog, err := readLog(os.Args[2])
			if err != nil {
				fmt.Println(err)
			}

			if err := freshnessVersions(writeLog, queryLog); err != nil {
				log.Fatal(err)
			}
		}
	}
}

func durationToMillis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}
