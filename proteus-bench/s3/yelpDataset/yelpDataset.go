package yelp

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/dvasilas/proteus/proteus_client"
)

// Object ...
type Object struct {
	Key    string
	Bucket string
	Md     map[string]string
}

type review struct {
	Stars      int    `json:"stars"`
	Votes      votes  `json:"votes"`
	UserID     string `json:"user_id"`
	ReviewID   string `json:"review_id"`
	Date       string `json:"date"`
	Datetime   time.Time
	Text       string `json:"text"`
	Type       string `json:"type"`
	BusinessID string `json:"business_id"`
}

type votes struct {
	Funny  int `json:"funny"`
	Useful int `json:"useful"`
	Cool   int `json:"cool"`
}

// Workload ...
type Workload struct {
	dataset []review
}

// New ...
func New() *Workload {
	rand.Seed(time.Now().UnixNano())
	return &Workload{
		dataset: make([]review, 0),
	}
}

// Update ...
func (w *Workload) Update(bucket string, updateTags func(string, string, map[string]string) error) error {
	reviewInd := rand.Intn(len(w.dataset))
	voteInd := rand.Intn(rand.Intn(3) + 1)
	op := rand.Float32()
	switch voteInd {
	case 0:
		if w.dataset[reviewInd].Votes.Funny == 0 {
			w.dataset[reviewInd].Votes.Funny++
		} else if op < 0.5 {
			w.dataset[reviewInd].Votes.Funny++
		} else {
			w.dataset[reviewInd].Votes.Funny--
		}
		return updateTags(bucket, w.dataset[reviewInd].ReviewID, map[string]string{"i-votes_funny": strconv.Itoa(w.dataset[reviewInd].Votes.Funny)})
	case 1:
		if w.dataset[reviewInd].Votes.Useful == 0 {
			w.dataset[reviewInd].Votes.Useful++
		} else if op < 0.5 {
			w.dataset[reviewInd].Votes.Useful++
		} else {
			w.dataset[reviewInd].Votes.Useful--
		}
		return updateTags(bucket, w.dataset[reviewInd].ReviewID, map[string]string{"i-votes_useful": strconv.Itoa(w.dataset[reviewInd].Votes.Useful)})
	case 2:
		if w.dataset[reviewInd].Votes.Cool == 0 {
			w.dataset[reviewInd].Votes.Cool++
		} else if op < 0.5 {
			w.dataset[reviewInd].Votes.Cool++
		} else {
			w.dataset[reviewInd].Votes.Cool--
		}
		return updateTags(bucket, w.dataset[reviewInd].ReviewID, map[string]string{"i-votes_cool": strconv.Itoa(w.dataset[reviewInd].Votes.Cool)})
	}
	return errors.New("any update")
}

// QueryPoint ...
func (w *Workload) QueryPoint() []proteusclient.AttributePredicate {
	reviewInd := rand.Intn(len(w.dataset))
	q := make([]proteusclient.AttributePredicate, 0)
	q = append(q, proteusclient.AttributePredicate{
		AttrName: "votes_useful",
		AttrType: proteusclient.S3TAGINT,
		Lbound:   int64(w.dataset[reviewInd].Votes.Useful),
		Ubound:   int64(w.dataset[reviewInd].Votes.Useful),
	})
	return q
}

// QueryRange ...
func (w *Workload) QueryRange() []proteusclient.AttributePredicate {
	reviewInd := rand.Intn(len(w.dataset))
	q := make([]proteusclient.AttributePredicate, 0)
	q = append(q, proteusclient.AttributePredicate{
		AttrName: "votes_useful",
		AttrType: proteusclient.S3TAGINT,
		Lbound:   int64(w.dataset[reviewInd].Votes.Useful),
		Ubound:   int64(w.dataset[reviewInd].Votes.Useful + 1),
	})
	return q
}

// PopulateDB ...
func (w *Workload) PopulateDB(bucket string, offset int64, datasetSize int64, putObject func(string, string, string, map[string]string) error) error {
	f, err := os.Open("./proteus-bench/s3/yelpDataset/dataset.json")
	if err != nil {
		return err
	}
	defer f.Close()

	var rev review
	scanner := bufio.NewScanner(f)
	for i := int64(0); i < offset && scanner.Scan(); i++ {
		if err := scanner.Err(); err != nil {
			return err
		}
	}
	for i := int64(0); i < datasetSize && scanner.Scan(); i++ {
		json.Unmarshal([]byte(scanner.Text()), &rev)
		rev.Datetime, err = time.Parse("2006-01-02", rev.Date)
		md := make(map[string]string, 0)
		md["i-stars"] = strconv.Itoa(rev.Stars)
		md["i-votes_cool"] = strconv.Itoa(rev.Votes.Cool)
		md["i-votes_funny"] = strconv.Itoa(rev.Votes.Funny)
		md["i-votes_useful"] = strconv.Itoa(rev.Votes.Useful)
		md["userID"] = rev.UserID
		md["date"] = rev.Datetime.String()
		md["type"] = rev.Type
		md["businessID"] = rev.BusinessID

		if putObject(bucket, rev.ReviewID, rev.Text, md) != nil {
			return err
		}
		if err := scanner.Err(); err != nil {
			return err
		}
	}
	return nil
}

// LoadDataset ..
func (w *Workload) LoadDataset() chan Object {
	ch := make(chan Object, 0)
	go func() {
		for object := range ch {
			stars, err := strconv.ParseInt(object.Md["i-stars"], 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			votesF, err := strconv.ParseInt(object.Md["i-votes_funny"], 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			votesU, err := strconv.ParseInt(object.Md["i-votes_useful"], 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			votesC, err := strconv.ParseInt(object.Md["i-votes_cool"], 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			dt, err := time.Parse("2006-01-02 00:00:00 +0000 UTC", object.Md["date"])
			if err != nil {
				log.Fatal(err)
			}
			r := review{
				Stars: int(stars),
				Votes: votes{
					Funny:  int(votesF),
					Useful: int(votesU),
					Cool:   int(votesC),
				},
				UserID:     object.Md["userid"],
				ReviewID:   object.Key,
				Datetime:   dt,
				Type:       object.Md["type"],
				BusinessID: object.Md["businessid"],
			}
			w.dataset = append(w.dataset, r)
		}
	}()
	return ch
}

// PrintDataset ...
func (w *Workload) PrintDataset() {
	fmt.Println("dataset")
	fmt.Println(len(w.dataset))
	// for _, r := range w.dataset {
	// 	fmt.Println(r.ReviewID)
	// 	fmt.Println(r.Votes)
	// }
}
