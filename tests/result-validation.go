package main

import (
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"time"

	"github.com/dvasilas/proteus/deployments"
	"github.com/dvasilas/proteus/internal/config"
	"github.com/dvasilas/proteus/internal/proto/qpu"
	"github.com/dvasilas/proteus/pkg/proteus-go-client"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// DeploymentConfig ...
type DeploymentConfig struct {
	Config []config.ConfJSON
}

// Config ...
type Config struct {
	Backend struct {
		BucketName string
		Endpoint   string
	}
	QueryEngine []struct {
		Host string
		Port int
	}
	Test struct {
		RecordCount int
		QueryCount  int
		UpdateCount int
	}
}

type database struct {
	tableName string
	endpoint  string
}

type storageState struct {
	state map[string]float64
	index map[float64][]string
}

func main() {
	initDebug()
	rand.Seed(time.Now().UnixNano())

	genFlags := flag.NewFlagSet("general flags", flag.ExitOnError)
	var configFile, deploymentFile string
	genFlags.StringVar(&configFile, "conf", "noArg", "test config file path")
	genFlags.StringVar(&deploymentFile, "depl", "noArg", "deployment config file path")
	genFlags.Parse(os.Args[1:3])

	var testConf Config
	if err := readConfigFile(configFile, &testConf); err != nil {
		log.Fatal(err)
	}

	var deploymentConf DeploymentConfig
	if err := readConfigFile(deploymentFile, &deploymentConf); err != nil {
		log.Fatal(err)
	}

	db := database{
		tableName: testConf.Backend.BucketName + fmt.Sprintf("%d", rand.Int()),
		endpoint:  testConf.Backend.Endpoint,
	}

	storageState := newStorageState()

	deploymentConf.Config[2].IndexConfig.Bucket = db.tableName

	// qpuServers := make([]*grpc.Server, len(deploymentConf.Config))
	qpus := make([]deployment.DeployedQPU, len(deploymentConf.Config))
	for i, conf := range deploymentConf.Config {
		qpu, err := deployment.Deploy(conf)
		if err != nil {
			log.Fatal(err)
		}
		qpus[i] = qpu
	}

	queryEngine := make([]*proteusclient.Client, 0)
	for _, endpoint := range testConf.QueryEngine {
		c, err := proteusclient.NewClient(proteusclient.Host{Name: endpoint.Host, Port: endpoint.Port})
		if err != nil {
			log.Fatal(err)
		}
		queryEngine = append(queryEngine, c)
	}

	if err := execCmd(exec.Command("touch", "/tmp/object"), true); err != nil {
		log.Fatal(err)
	}

	if err := db.createTable(); err != nil {
		log.Fatal(err)
	}

	if err := populateDatabase(db, testConf, storageState); err != nil {
		log.Fatal(err)
	}

	for i := 0; i < testConf.Test.QueryCount; i++ {
		if !doQuery(storageState, queryEngine, db) {
			log.Fatal(errors.New("wrong query result"))
		}
	}

	for i := 0; i < testConf.Test.UpdateCount; i++ {
		if err := updateRecord(db, storageState); err != nil {
			log.Fatal(err)
		}
		if !doQuery(storageState, queryEngine, db) {
			log.Fatal(errors.New("wrong query result"))
		}
	}

	time.Sleep(time.Second * 2)
	deployment.TearDown(qpus)
}

func doQuery(state *storageState, queryEngine []*proteusclient.Client, db database) bool {
	// lbound, ubound, expectedResult := newQuery(state)
	// log.WithFields(log.Fields{
	// 	"Lower bound": lbound,
	// 	"Upper bound": ubound,
	// }).Info("performing query")

	// responses := make([][]string, 0)
	// for _, q := range queryEngine {
	// 	resp, err := query(q, db.tableName, []proteusclient.AttributePredicate{
	// 		proteusclient.AttributePredicate{AttrName: "tripdistance", AttrType: proteusclient.S3TAGFLT, Lbound: lbound, Ubound: ubound},
	// 	}, proteusclient.LATESTSNAPSHOT)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	responses = append(responses, resp)
	// }
	// return validate(expectedResult, responses)
	return true
}

func readConfigFile(file string, conf interface{}) error {
	_, f, _, _ := runtime.Caller(0)
	basepath := filepath.Dir(f)
	viper.SetConfigName(file)
	viper.AddConfigPath(basepath)
	viper.AddConfigPath(basepath + "/conf")
	viper.SetConfigType("json")
	if err := viper.ReadInConfig(); err != nil {
		return err
	}
	if err := viper.Unmarshal(conf); err != nil {
		return err
	}
	return nil
}

func (st *storageState) query(lbound float64, ubound float64) []string {
	res := make([]string, 0)
	for k := range st.index {
		if k >= lbound && k < ubound {
			res = append(res, st.index[k]...)
		}
	}
	return res
}

func (st *storageState) insert(recordID string, attributeVal float64) {
	st.state[recordID] = attributeVal
	if _, ok := st.index[attributeVal]; !ok {
		st.index[attributeVal] = make([]string, 0)
	}
	st.index[attributeVal] = append(st.index[attributeVal], recordID)
}

func (st *storageState) update(recordID string, attributeVal float64) {
	oldVal := st.state[recordID]
	st.index[oldVal] = remove(st.index[oldVal], recordID)
	st.insert(recordID, attributeVal)
}

func populateDatabase(db database, conf Config, storageState *storageState) error {
	for i := 0; i < conf.Test.RecordCount; i++ {
		if err := insertRecord(db, storageState); err != nil {
			return err
		}
	}
	return nil
}

func newQuery(storageState *storageState) (float64, float64, []string) {
	for {
		lb := rand.Float64() * 10
		qRange := rand.Float64() * 10
		ub := 10.0
		if lb+qRange < 10.0 {
			ub = lb + qRange
		}
		expectedResult := storageState.query(lb, ub)
		if len(expectedResult) > 0 {
			return lb, ub, expectedResult
		}
	}
}

func insertRecord(db database, storageState *storageState) error {
	recordID := newRecordID()
	attributeVal := newAttributeVal()
	storageState.insert(recordID, attributeVal)
	return db.put(recordID, attributeVal)
}

func updateRecord(db database, storageState *storageState) error {
	recordID := getRandomRecord(storageState)
	attributeVal := newAttributeVal()
	storageState.update(recordID, attributeVal)
	return db.put(recordID, attributeVal)
}

func newStorageState() *storageState {
	return &storageState{
		state: make(map[string]float64),
		index: make(map[float64][]string),
	}
}

func (db database) put(recordID string, attribute float64) error {
	return execCmd(exec.Command("s3cmd", "put", "--host="+db.endpoint, "--add-header=x-amz-meta-f-tripdistance:"+fmt.Sprintf("%f", attribute), "/tmp/object", "s3://"+db.tableName+"/"+recordID), true)
}

func (db database) createTable() error {
	return execCmd(exec.Command("s3cmd", "mb", "s3://"+db.tableName+"/", "--host="+db.endpoint), true)
}

func getRandomRecord(storage *storageState) string {
	keys := reflect.ValueOf(storage.state).MapKeys()
	k := keys[rand.Intn(len(keys))].Interface()
	return fmt.Sprintf("%v", k)
}

func newRecordID() string {
	return "obj" + fmt.Sprintf("%d", rand.Int())
}

func newAttributeVal() float64 {
	return rand.Float64() * 10
}

func query(c *proteusclient.Client, bucket string, pred []qpu.AttributePredicate) ([]string, error) {
	// response := make([]string, 0)
	// respCh, errCh, err := c.Query(bucket, pred, qType, nil)
	// if err != nil {
	// 	return response, err
	// }
	// for {
	// 	select {
	// 	case err, ok := <-errCh:
	// 		if !ok {
	// 			errCh = nil
	// 		} else if err == io.EOF {
	// 		} else {
	// 			return response, err
	// 		}
	// 	case resp, ok := <-respCh:
	// 		if !ok {
	// 			respCh = nil
	// 		} else {
	// 			response = append(response, resp.ObjectID)
	// 		}
	// 	}
	// 	if errCh == nil && respCh == nil {
	// 		return response, err
	// 	}
	// }
	return nil, nil
}

func validate(expected []string, queryEngineResponses [][]string) bool {
	for _, resp := range queryEngineResponses {
		if !validateQueryResponse(expected, resp) {
			return false
		}
	}
	return true
}

func validateQueryResponse(expected, returned []string) bool {
	if len(expected) != len(returned) {
		return false
	}
	diff := make(map[string]int, len(expected))
	for _, x := range expected {
		diff[x]++
	}
	for _, y := range returned {
		if _, ok := diff[y]; !ok {
			return false
		}
		diff[y]--
		if diff[y] == 0 {
			delete(diff, y)
		}
	}
	if len(diff) == 0 {
		return true
	}
	return false
}

func initDebug() error {
	debug := os.Getenv("DEBUG")
	if debug == "true" {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	return nil
}

func remove(l []string, item string) []string {
	for i, other := range l {
		if other == item {
			return append(l[:i], l[i+1:]...)
		}
	}
	return l
}

func execCmd(cmd *exec.Cmd, wait bool) error {
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if wait {
		return cmd.Run()
	}
	return cmd.Start()
}
