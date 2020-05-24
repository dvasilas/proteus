package sumqpu

import (
	"fmt"
	"time"

	"github.com/dvasilas/proteus/internal/libqpu"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	qpugraph "github.com/dvasilas/proteus/internal/qpuGraph"
	"github.com/dvasilas/proteus/internal/queries"
	responsestream "github.com/dvasilas/proteus/internal/responseStream"

	//
	_ "github.com/go-sql-driver/mysql"
)

// SumQPU ...
type SumQPU struct {
	state            libqpu.QPUState
	schema           libqpu.Schema
	subscribeQueries []subscribeQuery
}

type subscribeQuery struct {
	query  libqpu.InternalQuery
	stream libqpu.RequestStream
	seqID  int64
}

// ---------------- API Functions -------------------

// InitClass ...
func InitClass(qpu *libqpu.QPU) (*SumQPU, error) {
	sqpu := &SumQPU{
		state:            qpu.State,
		schema:           qpu.Schema,
		subscribeQueries: make([]subscribeQuery, 0),
	}

	if err := sqpu.state.Init(
		"proteus",
		"stories_with_votes",
		"CREATE TABLE stories_with_votes (story_id bigint unsigned NOT NULL, comment_id bigint unsigned DEFAULT NULL, vote_count int, UNIQUE KEY story_comment (story_id, comment_id) )",
	); err != nil {
		return &SumQPU{}, err
	}

	querySnapshot := queries.GetSnapshot("votes", []string{"id", "story_id", "comment_id", "vote"}, []string{"comment_id"}, []string{})
	querySubscribe := queries.SubscribeToAllUpdates("votes", []string{"id", "story_id", "comment_id", "vote"}, []string{"comment_id"}, []string{})
	for _, adjQPU := range qpu.AdjacentQPUs {
		responseStream, err := qpugraph.SendQueryI(querySnapshot, adjQPU)
		if err != nil {
			return &SumQPU{}, err
		}

		if err = responsestream.StreamConsumer(responseStream, sqpu.processRespRecord, nil, nil); err != nil {
			return &SumQPU{}, err
		}

		recordCh := make(chan libqpu.ResponseRecord)
		responseStream, err = qpugraph.SendQueryI(querySubscribe, adjQPU)
		if err != nil {
			return &SumQPU{}, err
		}
		go func() {
			if err = responsestream.StreamConsumer(responseStream, sqpu.processRespRecord, nil, recordCh); err != nil {
				panic(err)
			}
		}()

		go func() {
			for respRecord := range recordCh {
				fmt.Println("here we need to forward ", respRecord)
				fmt.Println(sqpu.subscribeQueries)
			}
		}()

	}

	return sqpu, nil
}

// ProcessQuery ...
func (q *SumQPU) ProcessQuery(query libqpu.InternalQuery, stream libqpu.RequestStream, md map[string]string, sync bool) error {
	fmt.Println("ProcessQuery")
	if queries.IsSubscribeToAllQuery(query) {
		return q.subscribeQuery(query, stream)
	} else if queries.IsGetSnapshotQuery(query) {
		return q.snapshotQuery(query, stream)
	}
	return libqpu.Error("invalid query for datastore_driver QPU")
}

// ---------------- Internal Functions --------------

func (q *SumQPU) subscribeQuery(query libqpu.InternalQuery, stream libqpu.RequestStream) error {
	fmt.Println("subscribeQuery")

	q.subscribeQueries = append(q.subscribeQueries,
		subscribeQuery{
			query:  query,
			stream: stream,
			seqID:  0,
		},
	)
	fmt.Println(q.subscribeQueries)
	ch := make(chan int)
	<-ch
	return nil
}

func (q *SumQPU) snapshotQuery(query libqpu.InternalQuery, stream libqpu.RequestStream) error {
	fmt.Println("snapshotQuery")
	stateCh, err := q.state.Scan("stories_with_votes", []string{"story_id", "vote_count"})
	if err != nil {
		return err
	}

	var seqID int64
	for record := range stateCh {
		recordID := record["story_id"]
		fmt.Println(recordID)
		// delete(record, "story_id")

		attributes, err := q.schema.StrToAttributes(query.GetTable(), record)
		fmt.Println(record)
		fmt.Println(attributes, err)

		logOp := libqpu.LogOperationState(
			recordID,
			query.GetTable(),
			libqpu.Vectorclock(map[string]uint64{"tmp": uint64(time.Now().UnixNano())}),
			attributes,
		)
		ok, err := queries.SatisfiesPredicate(logOp, query)
		if err != nil {
			return err
		}
		if ok {
			fmt.Println("Sending ..")
			if err := stream.Send(seqID, libqpu.State, logOp); err != nil {
				return err
			}
			seqID++
		}
	}
	return stream.Send(
		seqID,
		libqpu.EndOfStream,
		libqpu.LogOperation{},
	)
}

func (q *SumQPU) processRespRecord(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord) error {
	if recordCh != nil {
		recordCh <- respRecord
	}

	attributes := respRecord.GetAttributes()
	storyID, err := q.schema.GetValue(attributes, "votes", "story_id")
	if err != nil {
		return err
	}
	vote, err := q.schema.GetValue(attributes, "votes", "vote")
	if err != nil {
		return err
	}
	var attributesNew map[string]*qpu.Value
	if !libqpu.HasAttribute(attributes, "comment_id") {
		attributesNew, err = q.processStoryVote(storyID.(int64), vote.(int64))
		fmt.Println(attributesNew)
		if err != nil {
			return err
		}
	} else {
		commentID, err := q.schema.GetValue(attributes, "votes", "comment_id")
		if err != nil {
			return err
		}

		attributesNew, err = q.processCommentVote(storyID.(int64), commentID.(int64), vote.(int64))
		if err != nil {
			return err
		}
	}

	fmt.Println(attributesNew)

	logOp := libqpu.LogOperationDelta(
		respRecord.GetRecordID(),
		"stories_with_votes",
		libqpu.Vectorclock(map[string]uint64{"TODO": uint64(time.Now().UnixNano())}),
		nil,
		attributesNew,
	)
	fmt.Println(logOp)

	for _, query := range q.subscribeQueries {
		ok, err := queries.SatisfiesPredicate(logOp, query.query)
		if err != nil {
			return err
		}
		if ok {
			fmt.Println("sending")
			if err := query.stream.Send(query.seqID, libqpu.Delta, logOp); err != nil {
				panic(err)
			}
			query.seqID++
		}
	}

	return nil
}

func (q *SumQPU) processStoryVote(storyID, vote int64) (map[string]*qpu.Value, error) {
	var newVoteCount int64
	voteCount, err := q.state.Get("vote_count", "stories_with_votes", "story_id = ? AND comment_id IS NULL",
		storyID)
	if err != nil && err.Error() == "sql: no rows in result set" {
		err = q.state.Insert("stories_with_votes", "(story_id, vote_count)", "(?,?)",
			storyID, vote)
		newVoteCount = vote
	} else {
		err = q.state.Update("stories_with_votes", "vote_count = ?", "story_id = ? AND comment_id IS NULL",
			voteCount.(int64)+vote, storyID)
		newVoteCount = voteCount.(int64) + vote
	}

	if err != nil {
		return nil, err
	}

	attributes := map[string]*qpu.Value{
		"story_id":   libqpu.ValueInt(storyID),
		"vote_count": libqpu.ValueInt(newVoteCount),
	}

	return attributes, err
}

func (q *SumQPU) processCommentVote(storyID, commentID, vote int64) (map[string]*qpu.Value, error) {
	var newVoteCount int64
	voteCount, err := q.state.Get("vote_count", "stories_with_votes", "story_id = ? AND comment_id = ?",
		storyID, commentID)
	if err != nil && err.Error() == "sql: no rows in result set" {
		err = q.state.Insert("stories_with_votes", "(story_id, comment_id, vote_count)", "(?,?,?)",
			storyID, commentID, vote)
		newVoteCount = vote
	} else {
		err = q.state.Update("stories_with_votes", "vote_count = ?", "story_id = ? AND comment_id = ?",
			voteCount.(int64)+vote, storyID, commentID)
		newVoteCount = voteCount.(int64) + vote
	}

	if err != nil {
		return nil, err
	}

	attributes := map[string]*qpu.Value{
		"story_id":   libqpu.ValueInt(storyID),
		"comment_id": libqpu.ValueInt(commentID),
		"vote_count": libqpu.ValueInt(newVoteCount),
	}

	return attributes, err
}
