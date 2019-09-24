package yelp

import (
	"bufio"
	"encoding/json"
	"errors"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/dvasilas/proteus/proteus_client"
)

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

	switch voteInd {
	case 0:
		w.dataset[reviewInd].Votes.Funny++
		return updateTags(bucket, w.dataset[reviewInd].ReviewID, map[string]string{"i-votes_funny": strconv.Itoa(w.dataset[reviewInd].Votes.Funny)})
	case 1:
		w.dataset[reviewInd].Votes.Useful++
		return updateTags(bucket, w.dataset[reviewInd].ReviewID, map[string]string{"i-votes_useful": strconv.Itoa(w.dataset[reviewInd].Votes.Useful)})
	case 2:
		w.dataset[reviewInd].Votes.Cool++
		return updateTags(bucket, w.dataset[reviewInd].ReviewID, map[string]string{"i-votes_cool": strconv.Itoa(w.dataset[reviewInd].Votes.Cool)})
	}
	return errors.New("any update")
}

// Query ...
func (w *Workload) Query() []proteusclient.AttributePredicate {
	q := make([]proteusclient.AttributePredicate, 0)
	q = append(q, proteusclient.AttributePredicate{
		AttrName: "vote_usefull",
		AttrType: proteusclient.S3TAGINT,
		Lbound:   int64(0),
		Ubound:   int64(10),
	})
	return q
}

// PopulateDB ...
func (w *Workload) PopulateDB(fName, bucket string, offset int64, datasetSize int64, doPopulate bool, putObject func(string, string, string, map[string]string) error) error {
	f, err := os.Open(fName)
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

		w.dataset = append(w.dataset, review{ReviewID: rev.ReviewID, Votes: rev.Votes})
		if doPopulate {
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
		}
		if err := scanner.Err(); err != nil {
			return err
		}
	}
	return nil
}
