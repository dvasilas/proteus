package joinqpu

import (
	"github.com/dvasilas/proteus/internal/libqpu"

	qpugraph "github.com/dvasilas/proteus/internal/qpuGraph"
	"github.com/dvasilas/proteus/internal/queries"
	responsestream "github.com/dvasilas/proteus/internal/responseStream"

	//
	_ "github.com/go-sql-driver/mysql"
)

// JoinQPU ...
type JoinQPU struct {
	state  libqpu.QPUState
	schema libqpu.Schema
}

// ---------------- API Functions -------------------

// InitClass ...
func InitClass(qpu *libqpu.QPU) (*JoinQPU, error) {
	jqpu := JoinQPU{
		state:  qpu.State,
		schema: qpu.Schema,
	}

	if err := jqpu.state.Init(
		"proteus",
		"stories",
		"CREATE TABLE stories (id bigint unsigned NOT NULL, user_id bigint unsigned NOT NULL, title varchar(150) NOT NULL DEFAULT '', description mediumtext, short_id varchar(6) NOT NULL DEFAULT '', vote_count int, PRIMARY KEY (id) )",
	); err != nil {
		return &JoinQPU{}, err
	}

	libqpu.Assert(len(qpu.AdjacentQPUs) == 2, "Join QPU should have two adjacent QPUs")

	querySnapshot := queries.GetSnapshot("stories", []string{"id", "user_id", "title", "description", "short_id"}, []string{}, []string{})
	querySubscribe := queries.SubscribeToAllUpdates("stories", []string{"id", "user_id", "title", "description", "short_id"}, []string{}, []string{})
	responseStreamStories, err := qpugraph.SendQueryI(querySnapshot, qpu.AdjacentQPUs[0])
	if err != nil {
		return &JoinQPU{}, err
	}
	if err = responsestream.StreamConsumer(responseStreamStories, jqpu.processStoriesRecord, nil, nil); err != nil {
		return &JoinQPU{}, err
	}
	// }()

	responseStreamStories, err = qpugraph.SendQueryI(querySubscribe, qpu.AdjacentQPUs[0])
	if err != nil {
		return &JoinQPU{}, err
	}
	go func() {
		if err = responsestream.StreamConsumer(responseStreamStories, jqpu.processStoriesRecord, nil, nil); err != nil {
			panic(err)
		}
	}()

	querySnapshot = queries.GetSnapshot("stateTable", []string{"story_id", "vote_sum"}, []string{}, []string{})
	querySubscribe = queries.SubscribeToAllUpdates("stateTable", []string{"story_id", "vote_sum"}, []string{}, []string{})
	responseStreamVoteCnt, err := qpugraph.SendQueryI(querySnapshot, qpu.AdjacentQPUs[1])
	if err != nil {
		return &JoinQPU{}, err
	}
	if err = responsestream.StreamConsumer(responseStreamVoteCnt, jqpu.processVoteCountRecord, nil, nil); err != nil {
		return &JoinQPU{}, err
	}

	responseStreamVoteCnt, err = qpugraph.SendQueryI(querySubscribe, qpu.AdjacentQPUs[1])
	if err != nil {
		return &JoinQPU{}, err
	}
	go func() {
		if err = responsestream.StreamConsumer(responseStreamVoteCnt, jqpu.processVoteCountRecord, nil, nil); err != nil {
			panic(err)
		}
	}()

	return &jqpu, nil
}

// ProcessQuery ...
func (q *JoinQPU) ProcessQuery(libqpu.InternalQuery, libqpu.RequestStream, map[string]string, bool) error {
	return nil
}

// ProcessQuerySnapshot ...
func (q *JoinQPU) ProcessQuerySnapshot(query libqpu.InternalQuery, stream libqpu.RequestStream, md map[string]string, sync bool) (<-chan libqpu.LogOperation, <-chan error) {
	// q.opConsumer(query, stream)
	return nil, nil
}

// ProcessQuerySubscribe ...
func (q *JoinQPU) ProcessQuerySubscribe(query libqpu.InternalQuery, stream libqpu.RequestStream, md map[string]string, sync bool) (<-chan libqpu.LogOperation, <-chan error) {
	// q.snapshotConsumer(query, stream)
	return nil, nil
}

// ---------------- Internal Functions --------------

func (q JoinQPU) processStoriesRecord(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord) error {
	libqpu.Trace("received record", map[string]interface{}{"record": respRecord})

	// attributes := respRecord.GetAttributes()
	// id := respRecord.GetRecordID()
	// userID, err := q.schema.GetValue(attributes, "stories", "user_id")
	// if err != nil {
	// 	return err
	// }
	// title, err := q.schema.GetValue(attributes, "stories", "title")
	// if err != nil {
	// 	return err
	// }
	// description, err := q.schema.GetValue(attributes, "stories", "description")
	// if err != nil {
	// 	return err
	// }
	// shortID, err := q.schema.GetValue(attributes, "stories", "short_id")
	// if err != nil {
	// 	return err
	// }

	// story, err := q.state.Get("*", "stories", "id = ?", id)
	// fmt.Println(story, err)
	// if err != nil && err.Error() == "sql: no rows in result set" {
	// 	return q.state.Insert("stories", "(id, user_id, title, description, short_id)", "(?,?,?,?,?)",
	// 		id, userID, title, description, shortID)
	// }
	return nil
}

func (q JoinQPU) processVoteCountRecord(respRecord libqpu.ResponseRecord, data interface{}, recordCh chan libqpu.ResponseRecord) error {
	libqpu.Trace("received record", map[string]interface{}{"record": respRecord})

	// attributes := respRecord.GetAttributes()
	// storyID := attributes["story_id"].GetInt()

	// voteCount, err := q.schema.GetValue(attributes, "stories_with_votes", "vote_count")
	// if err != nil {
	// 	return err
	// }
	// fmt.Println(storyID, voteCount)

	// return q.state.Update("stories", "vote_count = ?", "id = ?",
	// 	voteCount.(int64), storyID)
	return nil
}
