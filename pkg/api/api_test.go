package api

/*
These are "unit" tests that use gocqlmem and exercise ProcessDataBatchMsg implementation.
They all require copy_demo_date.sh to copy test data.
They take between 3 an 18 seconds each - this is why we do not include them in test_unit.sh
They test db-level Cassandra conditions, hence the somewhat comlex  TableInserterQueryPerformer interface.
*/
import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/capillariesio/capillaries/pkg/cql"
	"github.com/capillariesio/capillaries/pkg/ctx"
	"github.com/capillariesio/capillaries/pkg/custom/pycalc"
	"github.com/capillariesio/capillaries/pkg/custom/taganddenormalize"
	"github.com/capillariesio/capillaries/pkg/db"
	"github.com/capillariesio/capillaries/pkg/env"
	"github.com/capillariesio/capillaries/pkg/l"
	"github.com/capillariesio/capillaries/pkg/mq"
	"github.com/capillariesio/capillaries/pkg/sc"
	"github.com/capillariesio/capillaries/pkg/wfmodel"
	"github.com/capillariesio/gocqlmem/gocqlshims"
	"github.com/stretchr/testify/assert"
)

func readCSV(filename string) ([][]string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)

	var rows [][]string
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", filename, err)
		}
		rows = append(rows, row)
	}

	return rows, nil
}

func compareCsvs(file1 string, file2 string) error {
	a, err := readCSV(file1)
	if err != nil {
		return err
	}

	b, err := readCSV(file2)
	if err != nil {
		return err
	}

	maxRows := len(a)
	if len(b) > maxRows {
		maxRows = len(b)
	}

	for i := 0; i < maxRows; i++ {
		// Row exists only in a.csv.
		if i >= len(b) {
			return fmt.Errorf("Row %d: missing in the second csv: %q", i+1, a[i])
		}

		// Row exists only in b.csv.
		if i >= len(a) {
			return fmt.Errorf("Row %d: extra in the second csv: %q", i+1, b[i])
		}

		maxFields := len(a[i])
		if len(b[i]) > maxFields {
			maxFields = len(b[i])
		}

		for j := 0; j < maxFields; j++ {
			var av, bv string

			if j < len(a[i]) {
				av = a[i][j]
			}
			if j < len(b[i]) {
				bv = b[i][j]
			}

			if av != bv {
				return fmt.Errorf("Row %d, column %d: a.csv=%q, b.csv=%q", i+1, j+1, av, bv)
			}
		}
	}

	return nil
}

type TestProcessorDefFactory struct {
}

func (f *TestProcessorDefFactory) Create(processorType string) (sc.CustomProcessorDef, bool) {
	switch processorType {
	case pycalc.ProcessorPyCalcName:
		return &pycalc.PyCalcProcessorDef{}, true
	case taganddenormalize.ProcessorTagAndDenormalizeName:
		return &taganddenormalize.TagAndDenormalizeProcessorDef{}, true
	default:
		return nil, false
	}
}

// Table does not exist

type TableInserterQueryPerformerTestDataAndIdxDoesNotExist struct {
	TotalDataHits           int
	TotalIdxHits            int
	SimulatedDataErrorCount int
	SimulatedIdxErrorCount  int
}

func (qp *TableInserterQueryPerformerTestDataAndIdxDoesNotExist) PerformInsertDataRecordWithRowid(gocqlSession gocqlshims.Session, pq *cql.PreparedQuery, preparedDataQueryParams []any, retryCount int) (map[string]any, bool, error) {
	qp.TotalDataHits++
	if retryCount > 0 || qp.SimulatedDataErrorCount >= 1 {
		return db.HelperPerformInsertDataRecordWithRowid(gocqlSession, pq, preparedDataQueryParams, retryCount)
	} else {
		qp.SimulatedDataErrorCount++
		// log: will wait for table ... to be created, table retry count 0, got does not exist
		// retry and succeed
		return nil, false, fmt.Errorf("test scenario: data table " + cql.ErrorDoesNotExist)
	}
}

func (qp *TableInserterQueryPerformerTestDataAndIdxDoesNotExist) PerformInsertIdxRecordWithRowid(idxUniqueness sc.IdxUniqueness, gocqlSession gocqlshims.Session, pq *cql.PreparedQuery, preparedIdxQueryParams []any, retryCount int) (map[string]any, int, bool, error) {
	qp.TotalIdxHits++
	if retryCount > 0 || qp.SimulatedIdxErrorCount >= 1 {
		return db.HelperPerformInsertIdxRecordWithRowid(idxUniqueness, gocqlSession, pq, preparedIdxQueryParams, retryCount)
	} else {
		qp.SimulatedIdxErrorCount++
		// TEST ONLY
		//instr.DoesNotExistPauseMillis = 100      // speed things up for testing
		//instr.OperationTimedOutPauseMillis = 100 // speed things up for testing
		// log: will wait for idx table ... to be created, table retry count 0, got does not exist
		// retry and succeed
		return nil, retryCount, false, fmt.Errorf("test scenario: idx table " + cql.ErrorDoesNotExist)
	}
}

func TestTableDoesNotExist(t *testing.T) {
	os.Remove("/tmp/capi_out/lookup_quicktest/order_date_value_grouped_inner.csv")
	os.Remove("/tmp/capi_out/lookup_quicktest/order_date_value_grouped_left_outer.csv")
	os.Remove("/tmp/capi_out/lookup_quicktest/order_item_date_inner.csv")
	os.Remove("/tmp/capi_out/lookup_quicktest/order_item_date_left_outer.csv")

	ks := "ks_table_does_not_exist"

	envConfig := env.EnvConfig{
		Cassandra:                         env.CassandraConfig{WriterWorkers: 1},
		Log:                               env.LogConfig{Level: "INFO"},
		CustomProcessorDefFactoryInstance: &TestProcessorDefFactory{},
		UseGocqlmem:                       true,
	}
	sc.ScriptDefCache = sc.NewScriptDefCache()
	NodeDependencyReadynessCache = NewNodeDependencyReadynessCache()

	logger, err := l.NewLoggerFromEnvConfig(&envConfig, "unittest")
	assert.Nil(t, err)
	logger.PushF("TestTableDoesNotExist")
	defer logger.PopF()

	mqProducer := mq.TestInmemProducer{}

	gocqlmemSession, cassandraEngineType, err := db.NewSession(&envConfig, ks, db.CreateKeyspaceOnConnect)
	assert.Nil(t, err)

	_, err = StartRun(&envConfig, logger, &mqProducer, "/tmp/capi_cfg/lookup_quicktest/script_quick.yaml", "/tmp/capi_cfg/lookup_quicktest/script_params_quick_fs_one.yaml", gocqlmemSession, cassandraEngineType, ks, []string{"read_orders", "read_order_items"}, "test run")
	assert.Nil(t, err)

	var runStatus wfmodel.RunStatusType
	runHistory, err := GetRunHistory(gocqlmemSession, ks)
	assert.Nil(t, err)

	runStatus = runHistory[len(runHistory)-1].Status
	logger.Info("TestRun.RUNSTATUS: %s", runStatus.ToString())
	assert.Equal(t, wfmodel.RunStart, runStatus)

	var nodeRunStatus string
	for {
		msg := mqProducer.PeekHead()
		if msg == nil {
			break
		}
		queryPerformer := TableInserterQueryPerformerTestDataAndIdxDoesNotExist{}
		ackCmd := ProcessDataBatchMsg(&envConfig, logger, msg, 0, nil, ctx.TableInserterProperties{
			QueryPerformer:               &queryPerformer,
			DoesNotExistPauseMillis:      10, // speed it up for testing
			OperationTimedOutPauseMillis: 10, // speed it up for testing
		})
		if ackCmd == mq.AcknowledgerCmdAck {
			mqProducer.RemoveHead()
		} else {
			mqProducer.MoveHeadToTail()
		}
		runHistory, err := GetRunHistory(gocqlmemSession, ks)
		assert.Nil(t, err)

		runStatus = runHistory[len(runHistory)-1].Status
		logger.Info("TestRun.RUNSTATUS: %s", runStatus.ToString())

		nodeHistory, err := GetNodeHistoryForRuns(gocqlmemSession, ks, []int16{int16(1)})
		assert.Nil(t, err)

		newNodeRunStatusMap := map[string]wfmodel.NodeBatchStatusType{}
		for _, nodeEvent := range nodeHistory {
			newNodeRunStatusMap[nodeEvent.ScriptNode] = nodeEvent.Status
		}

		newNodeRunStatus := fmt.Sprintf("%v", newNodeRunStatusMap)
		if nodeRunStatus != newNodeRunStatus {
			nodeRunStatus = newNodeRunStatus
			logger.Info("TestRun.NODESTATUS: %s", nodeRunStatus)
		}

		// Make sure that fake errors were actually introduced
		if queryPerformer.TotalDataHits > 0 {
			if strings.HasPrefix(msg.TargetNodeName, "file_") {
				assert.Equal(t, 0, queryPerformer.SimulatedDataErrorCount, msg.TargetNodeName)
			} else {
				assert.Equal(t, 1, queryPerformer.SimulatedDataErrorCount, msg.TargetNodeName)
			}
		}

		if queryPerformer.TotalIdxHits > 0 {
			if strings.HasPrefix(msg.TargetNodeName, "read_") {
				assert.Equal(t, 1, queryPerformer.SimulatedIdxErrorCount, msg.TargetNodeName)
			} else {
				assert.Equal(t, 0, queryPerformer.SimulatedIdxErrorCount, msg.TargetNodeName)
			}
		}
	}
	assert.Equal(t, wfmodel.RunComplete, runStatus)

	err = compareCsvs("/tmp/capi_out/lookup_quicktest/order_date_value_grouped_inner_baseline.csv", "/tmp/capi_out/lookup_quicktest/order_date_value_grouped_inner.csv")
	assert.Nil(t, err)
	err = compareCsvs("/tmp/capi_out/lookup_quicktest/order_date_value_grouped_left_outer_baseline.csv", "/tmp/capi_out/lookup_quicktest/order_date_value_grouped_left_outer.csv")
	assert.Nil(t, err)
	err = compareCsvs("/tmp/capi_out/lookup_quicktest/order_item_date_inner_baseline.csv", "/tmp/capi_out/lookup_quicktest/order_item_date_inner.csv")
	assert.Nil(t, err)
	err = compareCsvs("/tmp/capi_out/lookup_quicktest/order_item_date_left_outer_baseline.csv", "/tmp/capi_out/lookup_quicktest/order_item_date_left_outer.csv")
	assert.Nil(t, err)

	gocqlmemSession.Query(fmt.Sprintf("DROP keyspace %s;", ks)).Exec()
	gocqlmemSession.Close()
}

// OperationTimedOut

type TableInserterQueryPerformerTestOperationTimedOut struct {
	TotalDataHits           int
	TotalIdxHits            int
	SimulatedDataErrorCount int
	SimulatedIdxErrorCount  int
}

func (qp *TableInserterQueryPerformerTestOperationTimedOut) PerformInsertDataRecordWithRowid(gocqlSession gocqlshims.Session, pq *cql.PreparedQuery, preparedDataQueryParams []any, retryCount int) (map[string]any, bool, error) {
	qp.TotalDataHits++
	if retryCount > 0 || qp.SimulatedDataErrorCount >= 1 {
		return db.HelperPerformInsertDataRecordWithRowid(gocqlSession, pq, preparedDataQueryParams, retryCount)
	} else {
		qp.SimulatedDataErrorCount++
		// log: cluster overloaded (Operation timed out), will wait for ...ms before writing to data table ... again, table retry count 0
		// retry and succeed
		return nil, false, fmt.Errorf("test scenario: data table " + cql.ErrorOperationTimedOut)
	}
}

func (qp *TableInserterQueryPerformerTestOperationTimedOut) PerformInsertIdxRecordWithRowid(idxUniqueness sc.IdxUniqueness, gocqlSession gocqlshims.Session, pq *cql.PreparedQuery, preparedIdxQueryParams []any, retryCount int) (map[string]any, int, bool, error) {
	qp.TotalIdxHits++
	if retryCount > 0 || qp.SimulatedIdxErrorCount >= 1 {
		return db.HelperPerformInsertIdxRecordWithRowid(idxUniqueness, gocqlSession, pq, preparedIdxQueryParams, retryCount)
	} else {
		qp.SimulatedIdxErrorCount++
		// log: cluster overloaded (Operation timed out), will wait for ...ms before writing to data table ... again, table retry count 0
		// retry and succeed
		return nil, retryCount, false, fmt.Errorf("test scenario: idx table " + cql.ErrorOperationTimedOut)
	}
}

func TestOperationTimedOut(t *testing.T) {
	os.Remove("/tmp/capi_out/lookup_quicktest/order_date_value_grouped_inner.csv")
	os.Remove("/tmp/capi_out/lookup_quicktest/order_date_value_grouped_left_outer.csv")
	os.Remove("/tmp/capi_out/lookup_quicktest/order_item_date_inner.csv")
	os.Remove("/tmp/capi_out/lookup_quicktest/order_item_date_left_outer.csv")

	ks := "ks_operation_timed_out"

	envConfig := env.EnvConfig{
		Cassandra:                         env.CassandraConfig{WriterWorkers: 1},
		Log:                               env.LogConfig{Level: "INFO"},
		CustomProcessorDefFactoryInstance: &TestProcessorDefFactory{},
		UseGocqlmem:                       true,
	}
	sc.ScriptDefCache = sc.NewScriptDefCache()
	NodeDependencyReadynessCache = NewNodeDependencyReadynessCache()

	logger, err := l.NewLoggerFromEnvConfig(&envConfig, "unittest")
	assert.Nil(t, err)
	logger.PushF("TestOperationTimedOut")
	defer logger.PopF()

	mqProducer := mq.TestInmemProducer{}

	gocqlmemSession, cassandraEngineType, err := db.NewSession(&envConfig, ks, db.CreateKeyspaceOnConnect)
	assert.Nil(t, err)

	_, err = StartRun(&envConfig, logger, &mqProducer, "/tmp/capi_cfg/lookup_quicktest/script_quick.yaml", "/tmp/capi_cfg/lookup_quicktest/script_params_quick_fs_one.yaml", gocqlmemSession, cassandraEngineType, ks, []string{"read_orders", "read_order_items"}, "test run")
	assert.Nil(t, err)

	var runStatus wfmodel.RunStatusType
	runHistory, err := GetRunHistory(gocqlmemSession, ks)
	assert.Nil(t, err)

	runStatus = runHistory[len(runHistory)-1].Status
	logger.Info("TestRun.RUNSTATUS: %s", runStatus.ToString())
	assert.Equal(t, wfmodel.RunStart, runStatus)

	var nodeRunStatus string
	for {
		msg := mqProducer.PeekHead()
		if msg == nil {
			break
		}
		queryPerformer := TableInserterQueryPerformerTestOperationTimedOut{}
		ackCmd := ProcessDataBatchMsg(&envConfig, logger, msg, 0, nil, ctx.TableInserterProperties{
			QueryPerformer:               &queryPerformer,
			DoesNotExistPauseMillis:      10, // speed it up for testing
			OperationTimedOutPauseMillis: 10, // speed it up for testing
		})
		if ackCmd == mq.AcknowledgerCmdAck {
			mqProducer.RemoveHead()
		} else {
			mqProducer.MoveHeadToTail()
		}
		runHistory, err := GetRunHistory(gocqlmemSession, ks)
		assert.Nil(t, err)

		runStatus = runHistory[len(runHistory)-1].Status
		logger.Info("TestRun.RUNSTATUS: %s", runStatus.ToString())

		nodeHistory, err := GetNodeHistoryForRuns(gocqlmemSession, ks, []int16{int16(1)})
		assert.Nil(t, err)

		newNodeRunStatusMap := map[string]wfmodel.NodeBatchStatusType{}
		for _, nodeEvent := range nodeHistory {
			newNodeRunStatusMap[nodeEvent.ScriptNode] = nodeEvent.Status
		}

		newNodeRunStatus := fmt.Sprintf("%v", newNodeRunStatusMap)
		if nodeRunStatus != newNodeRunStatus {
			nodeRunStatus = newNodeRunStatus
			logger.Info("TestRun.NODESTATUS: %s", nodeRunStatus)
		}

		// Make sure that fake errors were actually introduced
		if queryPerformer.TotalDataHits > 0 {
			if strings.HasPrefix(msg.TargetNodeName, "file_") {
				assert.Equal(t, 0, queryPerformer.SimulatedDataErrorCount, msg.TargetNodeName)
			} else {
				assert.Equal(t, 1, queryPerformer.SimulatedDataErrorCount, msg.TargetNodeName)
			}
		}

		if queryPerformer.TotalIdxHits > 0 {
			if strings.HasPrefix(msg.TargetNodeName, "read_") {
				assert.Equal(t, 1, queryPerformer.SimulatedIdxErrorCount, msg.TargetNodeName)
			} else {
				assert.Equal(t, 0, queryPerformer.SimulatedIdxErrorCount, msg.TargetNodeName)
			}
		}
	}
	assert.Equal(t, wfmodel.RunComplete, runStatus)

	err = compareCsvs("/tmp/capi_out/lookup_quicktest/order_date_value_grouped_inner_baseline.csv", "/tmp/capi_out/lookup_quicktest/order_date_value_grouped_inner.csv")
	assert.Nil(t, err)
	err = compareCsvs("/tmp/capi_out/lookup_quicktest/order_date_value_grouped_left_outer_baseline.csv", "/tmp/capi_out/lookup_quicktest/order_date_value_grouped_left_outer.csv")
	assert.Nil(t, err)
	err = compareCsvs("/tmp/capi_out/lookup_quicktest/order_item_date_inner_baseline.csv", "/tmp/capi_out/lookup_quicktest/order_item_date_inner.csv")
	assert.Nil(t, err)
	err = compareCsvs("/tmp/capi_out/lookup_quicktest/order_item_date_left_outer_baseline.csv", "/tmp/capi_out/lookup_quicktest/order_item_date_left_outer.csv")
	assert.Nil(t, err)

	gocqlmemSession.Query(fmt.Sprintf("DROP keyspace %s;", ks)).Exec()
	gocqlmemSession.Close()
}

// Data serious error

type TableInserterQueryPerformerTestDataSeriousError struct {
	TotalDataHits           int
	SimulatedDataErrorCount int
}

func (qp *TableInserterQueryPerformerTestDataSeriousError) PerformInsertDataRecordWithRowid(gocqlSession gocqlshims.Session, pq *cql.PreparedQuery, preparedDataQueryParams []any, retryCount int) (map[string]any, bool, error) {
	qp.TotalDataHits++
	if retryCount > 0 || qp.SimulatedDataErrorCount >= 1 {
		return db.HelperPerformInsertDataRecordWithRowid(gocqlSession, pq, preparedDataQueryParams, retryCount)
	} else {
		qp.SimulatedDataErrorCount++
		// UI: some serious error; cannot write to data table
		// give up immediately and report failure
		return nil, false, fmt.Errorf("test scenario: data table " + cql.ErrorSomeSeriousError)
	}
}

func (qp *TableInserterQueryPerformerTestDataSeriousError) PerformInsertIdxRecordWithRowid(idxUniqueness sc.IdxUniqueness, gocqlSession gocqlshims.Session, pq *cql.PreparedQuery, preparedIdxQueryParams []any, retryCount int) (map[string]any, int, bool, error) {
	return db.HelperPerformInsertIdxRecordWithRowid(idxUniqueness, gocqlSession, pq, preparedIdxQueryParams, retryCount)
}

func TestDataSeriousError(t *testing.T) {
	os.Remove("/tmp/capi_out/lookup_quicktest/order_date_value_grouped_inner.csv")
	os.Remove("/tmp/capi_out/lookup_quicktest/order_date_value_grouped_left_outer.csv")
	os.Remove("/tmp/capi_out/lookup_quicktest/order_item_date_inner.csv")
	os.Remove("/tmp/capi_out/lookup_quicktest/order_item_date_left_outer.csv")

	ks := "ks_data_serious_error"

	envConfig := env.EnvConfig{
		Cassandra:                         env.CassandraConfig{WriterWorkers: 1},
		Log:                               env.LogConfig{Level: "INFO"},
		CustomProcessorDefFactoryInstance: &TestProcessorDefFactory{},
		UseGocqlmem:                       true,
	}
	sc.ScriptDefCache = sc.NewScriptDefCache()
	NodeDependencyReadynessCache = NewNodeDependencyReadynessCache()

	logger, err := l.NewLoggerFromEnvConfig(&envConfig, "unittest")
	assert.Nil(t, err)
	logger.PushF("TestDataSeriousError")
	defer logger.PopF()

	mqProducer := mq.TestInmemProducer{}

	gocqlmemSession, cassandraEngineType, err := db.NewSession(&envConfig, ks, db.CreateKeyspaceOnConnect)
	assert.Nil(t, err)

	_, err = StartRun(&envConfig, logger, &mqProducer, "/tmp/capi_cfg/lookup_quicktest/script_quick.yaml", "/tmp/capi_cfg/lookup_quicktest/script_params_quick_fs_one.yaml", gocqlmemSession, cassandraEngineType, ks, []string{"read_orders", "read_order_items"}, "test run")
	assert.Nil(t, err)

	var runStatus wfmodel.RunStatusType
	runHistory, err := GetRunHistory(gocqlmemSession, ks)
	assert.Nil(t, err)

	runStatus = runHistory[len(runHistory)-1].Status
	logger.Info("TestRun.RUNSTATUS: %s", runStatus.ToString())
	assert.Equal(t, wfmodel.RunStart, runStatus)

	//var nodeRunStatus string
	for {
		msg := mqProducer.PeekHead()
		if msg == nil {
			break
		}
		queryPerformer := TableInserterQueryPerformerTestDataSeriousError{}
		ackCmd := ProcessDataBatchMsg(&envConfig, logger, msg, 0, nil, ctx.TableInserterProperties{
			QueryPerformer:               &queryPerformer,
			DoesNotExistPauseMillis:      10, // speed it up for testing
			OperationTimedOutPauseMillis: 10, // speed it up for testing
		})
		if ackCmd == mq.AcknowledgerCmdAck {
			mqProducer.RemoveHead()
		} else {
			mqProducer.MoveHeadToTail()
		}
	}

	// Verify run status
	runHistory, err = GetRunHistory(gocqlmemSession, ks)
	assert.Nil(t, err)
	runStatus = runHistory[len(runHistory)-1].Status
	assert.Equal(t, wfmodel.RunComplete, runStatus)

	// Verify node statuses
	nodeHistory, err := GetNodeHistoryForRuns(gocqlmemSession, ks, []int16{int16(1)})
	assert.Nil(t, err)
	newNodeRunStatusMap := map[string]wfmodel.NodeBatchStatusType{}
	for _, nodeEvent := range nodeHistory {
		newNodeRunStatusMap[nodeEvent.ScriptNode] = nodeEvent.Status
	}
	logger.Info("TestRun.NODESTATUS final: %s", fmt.Sprintf("%v", newNodeRunStatusMap))

	// For each node, verify batch statuses
	for nodeName, nodeStatus := range newNodeRunStatusMap {
		assert.Equal(t, wfmodel.NodeBatchFail, nodeStatus, fmt.Sprintf("node %s supposed to fail", nodeName))
		// Make sure all batches for this node started then failed
		batchEvents, err := GetBatchHistoryForRunAndNode(gocqlmemSession, ks, int16(1), nodeName)
		assert.Nil(t, err)
		if nodeName == "read_orders" || nodeName == "read_order_items" {
			// These nodes start and fail
			assert.Equal(t, 2, len(batchEvents), nodeName)
			assert.Equal(t, wfmodel.NodeBatchStart, batchEvents[0].Status, nodeName)
			assert.Equal(t, wfmodel.NodeBatchFail, batchEvents[1].Status, nodeName)
			assert.True(t, strings.Contains(batchEvents[1].Comment, cql.ErrorSomeSeriousError))
		} else {
			// These nodes failed without starting
			for _, event := range batchEvents {
				assert.Equal(t, wfmodel.NodeBatchFail, event.Status, nodeName)
				assert.True(t, strings.Contains(event.Comment, "some dependency nodes"))
			}
		}
	}
	gocqlmemSession.Query(fmt.Sprintf("DROP keyspace %s;", ks)).Exec()
	gocqlmemSession.Close()
}

// Idx serious error

type TableInserterQueryPerformerTestIdxSeriousError struct {
	TotalIdxHits           int
	SimulatedIdxErrorCount int
}

func (qp *TableInserterQueryPerformerTestIdxSeriousError) PerformInsertDataRecordWithRowid(gocqlSession gocqlshims.Session, pq *cql.PreparedQuery, preparedDataQueryParams []any, retryCount int) (map[string]any, bool, error) {
	return db.HelperPerformInsertDataRecordWithRowid(gocqlSession, pq, preparedDataQueryParams, retryCount)
}

func (qp *TableInserterQueryPerformerTestIdxSeriousError) PerformInsertIdxRecordWithRowid(idxUniqueness sc.IdxUniqueness, gocqlSession gocqlshims.Session, pq *cql.PreparedQuery, preparedIdxQueryParams []any, retryCount int) (map[string]any, int, bool, error) {
	qp.TotalIdxHits++
	if retryCount > 0 || qp.SimulatedIdxErrorCount >= 1 {
		return db.HelperPerformInsertIdxRecordWithRowid(idxUniqueness, gocqlSession, pq, preparedIdxQueryParams, retryCount)
	} else {
		qp.SimulatedIdxErrorCount++
		// UI: some serious error; cannot write to idx table
		// give up immediately and report failure
		return nil, retryCount, false, fmt.Errorf("test scenario: idx table " + cql.ErrorSomeSeriousError)
	}
}

func TestIdxSeriousError(t *testing.T) {
	os.Remove("/tmp/capi_out/lookup_quicktest/order_date_value_grouped_inner.csv")
	os.Remove("/tmp/capi_out/lookup_quicktest/order_date_value_grouped_left_outer.csv")
	os.Remove("/tmp/capi_out/lookup_quicktest/order_item_date_inner.csv")
	os.Remove("/tmp/capi_out/lookup_quicktest/order_item_date_left_outer.csv")

	ks := "ks_idx_serious_error"

	envConfig := env.EnvConfig{
		Cassandra:                         env.CassandraConfig{WriterWorkers: 1},
		Log:                               env.LogConfig{Level: "INFO"},
		CustomProcessorDefFactoryInstance: &TestProcessorDefFactory{},
		UseGocqlmem:                       true,
	}
	sc.ScriptDefCache = sc.NewScriptDefCache()
	NodeDependencyReadynessCache = NewNodeDependencyReadynessCache()

	logger, err := l.NewLoggerFromEnvConfig(&envConfig, "unittest")
	assert.Nil(t, err)
	logger.PushF("TestIdxSeriousError")
	defer logger.PopF()

	mqProducer := mq.TestInmemProducer{}

	gocqlmemSession, cassandraEngineType, err := db.NewSession(&envConfig, ks, db.CreateKeyspaceOnConnect)
	assert.Nil(t, err)

	_, err = StartRun(&envConfig, logger, &mqProducer, "/tmp/capi_cfg/lookup_quicktest/script_quick.yaml", "/tmp/capi_cfg/lookup_quicktest/script_params_quick_fs_one.yaml", gocqlmemSession, cassandraEngineType, ks, []string{"read_orders", "read_order_items"}, "test run")
	assert.Nil(t, err)

	var runStatus wfmodel.RunStatusType
	runHistory, err := GetRunHistory(gocqlmemSession, ks)
	assert.Nil(t, err)

	runStatus = runHistory[len(runHistory)-1].Status
	logger.Info("TestRun.RUNSTATUS: %s", runStatus.ToString())
	assert.Equal(t, wfmodel.RunStart, runStatus)

	//var nodeRunStatus string
	for {
		msg := mqProducer.PeekHead()
		if msg == nil {
			break
		}
		queryPerformer := TableInserterQueryPerformerTestIdxSeriousError{}
		ackCmd := ProcessDataBatchMsg(&envConfig, logger, msg, 0, nil, ctx.TableInserterProperties{
			QueryPerformer:               &queryPerformer,
			DoesNotExistPauseMillis:      10, // speed it up for testing
			OperationTimedOutPauseMillis: 10, // speed it up for testing
		})
		if ackCmd == mq.AcknowledgerCmdAck {
			mqProducer.RemoveHead()
		} else {
			mqProducer.MoveHeadToTail()
		}
	}

	// Verify run status
	runHistory, err = GetRunHistory(gocqlmemSession, ks)
	assert.Nil(t, err)
	runStatus = runHistory[len(runHistory)-1].Status
	assert.Equal(t, wfmodel.RunComplete, runStatus)

	// Verify node statuses
	nodeHistory, err := GetNodeHistoryForRuns(gocqlmemSession, ks, []int16{int16(1)})
	assert.Nil(t, err)
	newNodeRunStatusMap := map[string]wfmodel.NodeBatchStatusType{}
	for _, nodeEvent := range nodeHistory {
		newNodeRunStatusMap[nodeEvent.ScriptNode] = nodeEvent.Status
	}
	logger.Info("TestRun.NODESTATUS final: %s", fmt.Sprintf("%v", newNodeRunStatusMap))

	// For each node, verify batch statuses
	for nodeName, nodeStatus := range newNodeRunStatusMap {
		assert.Equal(t, wfmodel.NodeBatchFail, nodeStatus, fmt.Sprintf("node %s supposed to fail", nodeName))
		// Make sure all batches for this node started then failed
		batchEvents, err := GetBatchHistoryForRunAndNode(gocqlmemSession, ks, int16(1), nodeName)
		assert.Nil(t, err)
		if nodeName == "read_orders" || nodeName == "read_order_items" {
			// These nodes start and fail
			assert.Equal(t, 2, len(batchEvents), nodeName)
			assert.Equal(t, wfmodel.NodeBatchStart, batchEvents[0].Status, nodeName)
			assert.Equal(t, wfmodel.NodeBatchFail, batchEvents[1].Status, nodeName)
			assert.True(t, strings.Contains(batchEvents[1].Comment, cql.ErrorSomeSeriousError))
		} else {
			// These nodes failed without starting
			for _, event := range batchEvents {
				assert.Equal(t, wfmodel.NodeBatchFail, event.Status, nodeName)
				assert.True(t, strings.Contains(event.Comment, "some dependency nodes"))
			}
		}
	}
	gocqlmemSession.Query(fmt.Sprintf("DROP keyspace %s;", ks)).Exec()
	gocqlmemSession.Close()
}

// Data not applied

type TableInserterQueryPerformerTestDataNotApplied struct {
	TotalDataHits           int
	SimulatedDataErrorCount int
}

func (qp *TableInserterQueryPerformerTestDataNotApplied) PerformInsertDataRecordWithRowid(gocqlSession gocqlshims.Session, pq *cql.PreparedQuery, preparedDataQueryParams []any, retryCount int) (map[string]any, bool, error) {
	qp.TotalDataHits++
	if retryCount > 0 || qp.SimulatedDataErrorCount >= 1 {
		return db.HelperPerformInsertDataRecordWithRowid(gocqlSession, pq, preparedDataQueryParams, retryCount)
	} else {
		qp.SimulatedDataErrorCount++
		// log warning: duplicate rowid not written [INSERT INTO ...], existing record [...], table retry count 0
		// This will trigger non-fatal ErrDuplicateRowid error
		// 	retry with new rowid and succeed
		// 	isApplied = false
		return nil, false, nil
	}
}

func (qp *TableInserterQueryPerformerTestDataNotApplied) PerformInsertIdxRecordWithRowid(idxUniqueness sc.IdxUniqueness, gocqlSession gocqlshims.Session, pq *cql.PreparedQuery, preparedIdxQueryParams []any, retryCount int) (map[string]any, int, bool, error) {
	return db.HelperPerformInsertIdxRecordWithRowid(idxUniqueness, gocqlSession, pq, preparedIdxQueryParams, retryCount)
}

func TestDataNotApplied(t *testing.T) {
	os.Remove("/tmp/capi_out/lookup_quicktest/order_date_value_grouped_inner.csv")
	os.Remove("/tmp/capi_out/lookup_quicktest/order_date_value_grouped_left_outer.csv")
	os.Remove("/tmp/capi_out/lookup_quicktest/order_item_date_inner.csv")
	os.Remove("/tmp/capi_out/lookup_quicktest/order_item_date_left_outer.csv")

	ks := "ks_data_not_applied"

	envConfig := env.EnvConfig{
		Cassandra:                         env.CassandraConfig{WriterWorkers: 1},
		Log:                               env.LogConfig{Level: "INFO"},
		CustomProcessorDefFactoryInstance: &TestProcessorDefFactory{},
		UseGocqlmem:                       true,
	}
	sc.ScriptDefCache = sc.NewScriptDefCache()
	NodeDependencyReadynessCache = NewNodeDependencyReadynessCache()

	logger, err := l.NewLoggerFromEnvConfig(&envConfig, "unittest")
	assert.Nil(t, err)
	logger.PushF("TestDataNotApplied")
	defer logger.PopF()

	mqProducer := mq.TestInmemProducer{}

	gocqlmemSession, cassandraEngineType, err := db.NewSession(&envConfig, ks, db.CreateKeyspaceOnConnect)
	assert.Nil(t, err)

	_, err = StartRun(&envConfig, logger, &mqProducer, "/tmp/capi_cfg/lookup_quicktest/script_quick.yaml", "/tmp/capi_cfg/lookup_quicktest/script_params_quick_fs_one.yaml", gocqlmemSession, cassandraEngineType, ks, []string{"read_orders", "read_order_items"}, "test run")
	assert.Nil(t, err)

	var runStatus wfmodel.RunStatusType
	runHistory, err := GetRunHistory(gocqlmemSession, ks)
	assert.Nil(t, err)

	runStatus = runHistory[len(runHistory)-1].Status
	logger.Info("TestRun.RUNSTATUS: %s", runStatus.ToString())
	assert.Equal(t, wfmodel.RunStart, runStatus)

	var nodeRunStatus string
	for {
		msg := mqProducer.PeekHead()
		if msg == nil {
			break
		}
		queryPerformer := TableInserterQueryPerformerTestDataNotApplied{}
		ackCmd := ProcessDataBatchMsg(&envConfig, logger, msg, 0, nil, ctx.TableInserterProperties{
			QueryPerformer:               &queryPerformer,
			DoesNotExistPauseMillis:      10, // speed it up for testing
			OperationTimedOutPauseMillis: 10, // speed it up for testing
		})
		if ackCmd == mq.AcknowledgerCmdAck {
			mqProducer.RemoveHead()
		} else {
			mqProducer.MoveHeadToTail()
		}
		runHistory, err := GetRunHistory(gocqlmemSession, ks)
		assert.Nil(t, err)

		runStatus = runHistory[len(runHistory)-1].Status
		logger.Info("TestRun.RUNSTATUS: %s", runStatus.ToString())

		nodeHistory, err := GetNodeHistoryForRuns(gocqlmemSession, ks, []int16{int16(1)})
		assert.Nil(t, err)

		newNodeRunStatusMap := map[string]wfmodel.NodeBatchStatusType{}
		for _, nodeEvent := range nodeHistory {
			newNodeRunStatusMap[nodeEvent.ScriptNode] = nodeEvent.Status
		}

		newNodeRunStatus := fmt.Sprintf("%v", newNodeRunStatusMap)
		if nodeRunStatus != newNodeRunStatus {
			nodeRunStatus = newNodeRunStatus
			logger.Info("TestRun.NODESTATUS: %s", nodeRunStatus)
		}

		// Make sure that fake errors were actually introduced
		if queryPerformer.TotalDataHits > 0 {
			if strings.HasPrefix(msg.TargetNodeName, "file_") {
				assert.Equal(t, 0, queryPerformer.SimulatedDataErrorCount, msg.TargetNodeName)
			} else {
				assert.Equal(t, 1, queryPerformer.SimulatedDataErrorCount, msg.TargetNodeName)
			}
		}
	}
	assert.Equal(t, wfmodel.RunComplete, runStatus)

	err = compareCsvs("/tmp/capi_out/lookup_quicktest/order_date_value_grouped_inner_baseline.csv", "/tmp/capi_out/lookup_quicktest/order_date_value_grouped_inner.csv")
	assert.Nil(t, err)
	err = compareCsvs("/tmp/capi_out/lookup_quicktest/order_date_value_grouped_left_outer_baseline.csv", "/tmp/capi_out/lookup_quicktest/order_date_value_grouped_left_outer.csv")
	assert.Nil(t, err)
	err = compareCsvs("/tmp/capi_out/lookup_quicktest/order_item_date_inner_baseline.csv", "/tmp/capi_out/lookup_quicktest/order_item_date_inner.csv")
	assert.Nil(t, err)
	err = compareCsvs("/tmp/capi_out/lookup_quicktest/order_item_date_left_outer_baseline.csv", "/tmp/capi_out/lookup_quicktest/order_item_date_left_outer.csv")
	assert.Nil(t, err)

	gocqlmemSession.Query(fmt.Sprintf("DROP keyspace %s;", ks)).Exec()
	gocqlmemSession.Close()
}

// IdxNotAppliedSamePresentFirstRetry

type TableInserterQueryPerformerTestIdxNotAppliedSamePresentFirstRetry struct {
	TotalIdxHits           int
	SimulatedIdxErrorCount int
}

func (qp *TableInserterQueryPerformerTestIdxNotAppliedSamePresentFirstRetry) PerformInsertDataRecordWithRowid(gocqlSession gocqlshims.Session, pq *cql.PreparedQuery, preparedDataQueryParams []any, retryCount int) (map[string]any, bool, error) {
	return db.HelperPerformInsertDataRecordWithRowid(gocqlSession, pq, preparedDataQueryParams, retryCount)
}

func (qp *TableInserterQueryPerformerTestIdxNotAppliedSamePresentFirstRetry) PerformInsertIdxRecordWithRowid(idxUniqueness sc.IdxUniqueness, gocqlSession gocqlshims.Session, pq *cql.PreparedQuery, preparedIdxQueryParams []any, retryCount int) (map[string]any, int, bool, error) {
	qp.TotalIdxHits++
	if retryCount > 0 || qp.SimulatedIdxErrorCount >= 1 {
		return db.HelperPerformInsertIdxRecordWithRowid(idxUniqueness, gocqlSession, pq, preparedIdxQueryParams, retryCount)
	} else {
		qp.SimulatedIdxErrorCount++
		// log: cannot write duplicate index key [%s] and proper rowid with %s,%d on retry 0, existing record [%v], assuming it was some other writer, throwing error %w
		// give up immediately and report failure
		existingIdxRow := map[string]any{}
		existingIdxRow["key"] = pq.Qb.PreparedColumnData.Values[pq.Qb.PreparedColumnData.ColumnIdxMap["key"]]
		existingIdxRow["rowid"] = pq.Qb.PreparedColumnData.Values[pq.Qb.PreparedColumnData.ColumnIdxMap["rowid"]]
		return existingIdxRow, retryCount, false, nil
	}
}

func TestIdxNotAppliedSamePresentFirstRetry(t *testing.T) {
	os.Remove("/tmp/capi_out/lookup_quicktest/order_date_value_grouped_inner.csv")
	os.Remove("/tmp/capi_out/lookup_quicktest/order_date_value_grouped_left_outer.csv")
	os.Remove("/tmp/capi_out/lookup_quicktest/order_item_date_inner.csv")
	os.Remove("/tmp/capi_out/lookup_quicktest/order_item_date_left_outer.csv")

	ks := "ks_idx_not_applied_same_present_first_retry"

	envConfig := env.EnvConfig{
		Cassandra:                         env.CassandraConfig{WriterWorkers: 1},
		Log:                               env.LogConfig{Level: "INFO"},
		CustomProcessorDefFactoryInstance: &TestProcessorDefFactory{},
		UseGocqlmem:                       true,
	}
	sc.ScriptDefCache = sc.NewScriptDefCache()
	NodeDependencyReadynessCache = NewNodeDependencyReadynessCache()

	logger, err := l.NewLoggerFromEnvConfig(&envConfig, "unittest")
	assert.Nil(t, err)
	logger.PushF("TestIdxSeriousError")
	defer logger.PopF()

	mqProducer := mq.TestInmemProducer{}

	gocqlmemSession, cassandraEngineType, err := db.NewSession(&envConfig, ks, db.CreateKeyspaceOnConnect)
	assert.Nil(t, err)

	_, err = StartRun(&envConfig, logger, &mqProducer, "/tmp/capi_cfg/lookup_quicktest/script_quick.yaml", "/tmp/capi_cfg/lookup_quicktest/script_params_quick_fs_one.yaml", gocqlmemSession, cassandraEngineType, ks, []string{"read_orders", "read_order_items"}, "test run")
	assert.Nil(t, err)

	var runStatus wfmodel.RunStatusType
	runHistory, err := GetRunHistory(gocqlmemSession, ks)
	assert.Nil(t, err)

	runStatus = runHistory[len(runHistory)-1].Status
	logger.Info("TestRun.RUNSTATUS: %s", runStatus.ToString())
	assert.Equal(t, wfmodel.RunStart, runStatus)

	//var nodeRunStatus string
	for {
		msg := mqProducer.PeekHead()
		if msg == nil {
			break
		}
		queryPerformer := TableInserterQueryPerformerTestIdxNotAppliedSamePresentFirstRetry{}
		ackCmd := ProcessDataBatchMsg(&envConfig, logger, msg, 0, nil, ctx.TableInserterProperties{
			QueryPerformer:               &queryPerformer,
			DoesNotExistPauseMillis:      10, // speed it up for testing
			OperationTimedOutPauseMillis: 10, // speed it up for testing
		})
		if ackCmd == mq.AcknowledgerCmdAck {
			mqProducer.RemoveHead()
		} else {
			mqProducer.MoveHeadToTail()
		}
	}

	// Verify run status
	runHistory, err = GetRunHistory(gocqlmemSession, ks)
	assert.Nil(t, err)
	runStatus = runHistory[len(runHistory)-1].Status
	assert.Equal(t, wfmodel.RunComplete, runStatus)

	// Verify node statuses
	nodeHistory, err := GetNodeHistoryForRuns(gocqlmemSession, ks, []int16{int16(1)})
	assert.Nil(t, err)
	newNodeRunStatusMap := map[string]wfmodel.NodeBatchStatusType{}
	for _, nodeEvent := range nodeHistory {
		newNodeRunStatusMap[nodeEvent.ScriptNode] = nodeEvent.Status
	}
	logger.Info("TestRun.NODESTATUS final: %s", fmt.Sprintf("%v", newNodeRunStatusMap))

	// For each node, verify batch statuses
	for nodeName, nodeStatus := range newNodeRunStatusMap {
		assert.Equal(t, wfmodel.NodeBatchFail, nodeStatus, fmt.Sprintf("node %s supposed to fail", nodeName))
		// Make sure all batches for this node started then failed
		batchEvents, err := GetBatchHistoryForRunAndNode(gocqlmemSession, ks, int16(1), nodeName)
		assert.Nil(t, err)
		if nodeName == "read_orders" || nodeName == "read_order_items" {
			// These nodes start and fail
			assert.Equal(t, 2, len(batchEvents), nodeName)
			assert.Equal(t, wfmodel.NodeBatchStart, batchEvents[0].Status, nodeName)
			assert.Equal(t, wfmodel.NodeBatchFail, batchEvents[1].Status, nodeName)
			assert.True(t, strings.Contains(batchEvents[1].Comment, "assuming it was some other writer, throwing error duplicate key"))
		} else {
			// These nodes failed without starting
			for _, event := range batchEvents {
				assert.Equal(t, wfmodel.NodeBatchFail, event.Status, nodeName)
				assert.True(t, strings.Contains(event.Comment, "some dependency nodes"))
			}
		}
	}
	gocqlmemSession.Query(fmt.Sprintf("DROP keyspace %s;", ks)).Exec()
	gocqlmemSession.Close()
}

// IdxNotAppliedSamePresentSecondRetry

type TableInserterQueryPerformerTestIdxNotAppliedSamePresentSecondRetry struct {
	TotalIdxHits           int
	SimulatedIdxErrorCount int
}

func (qp *TableInserterQueryPerformerTestIdxNotAppliedSamePresentSecondRetry) PerformInsertDataRecordWithRowid(gocqlSession gocqlshims.Session, pq *cql.PreparedQuery, preparedDataQueryParams []any, retryCount int) (map[string]any, bool, error) {
	return db.HelperPerformInsertDataRecordWithRowid(gocqlSession, pq, preparedDataQueryParams, retryCount)
}

func (qp *TableInserterQueryPerformerTestIdxNotAppliedSamePresentSecondRetry) PerformInsertIdxRecordWithRowid(idxUniqueness sc.IdxUniqueness, gocqlSession gocqlshims.Session, pq *cql.PreparedQuery, preparedIdxQueryParams []any, retryCount int) (map[string]any, int, bool, error) {
	qp.TotalIdxHits++
	if retryCount > 0 || qp.SimulatedIdxErrorCount >= 1 {
		return db.HelperPerformInsertIdxRecordWithRowid(idxUniqueness, gocqlSession, pq, preparedIdxQueryParams, retryCount)
	} else {
		qp.SimulatedIdxErrorCount++
		// log: duplicate idx record found (%s) in idx %s on retry %d when writing (%d,'%s'), assuming this retry was successful, proceeding as usual
		// consider it a success
		// Simulate first successful attempt:
		db.HelperPerformInsertIdxRecordWithRowid(idxUniqueness, gocqlSession, pq, preparedIdxQueryParams, retryCount)
		// Simulate second attempt with isApplied=false:
		existingIdxRow := map[string]any{}
		existingIdxRow["key"] = pq.Qb.PreparedColumnData.Values[pq.Qb.PreparedColumnData.ColumnIdxMap["key"]]
		existingIdxRow["rowid"] = pq.Qb.PreparedColumnData.Values[pq.Qb.PreparedColumnData.ColumnIdxMap["rowid"]]
		// Return retry count 1, not 0. This will make inserter believe we are writing twice the same data, which is a happy (well, relatively) path
		return existingIdxRow, 1, false, nil
	}
}

func TestIdxNotAppliedSamePresentSecondRetry(t *testing.T) {
	os.Remove("/tmp/capi_out/lookup_quicktest/order_date_value_grouped_inner.csv")
	os.Remove("/tmp/capi_out/lookup_quicktest/order_date_value_grouped_left_outer.csv")
	os.Remove("/tmp/capi_out/lookup_quicktest/order_item_date_inner.csv")
	os.Remove("/tmp/capi_out/lookup_quicktest/order_item_date_left_outer.csv")

	ks := "ks_idx_not_applied_same_present_second_retry"

	envConfig := env.EnvConfig{
		Cassandra:                         env.CassandraConfig{WriterWorkers: 1},
		Log:                               env.LogConfig{Level: "INFO"},
		CustomProcessorDefFactoryInstance: &TestProcessorDefFactory{},
		UseGocqlmem:                       true,
	}
	sc.ScriptDefCache = sc.NewScriptDefCache()
	NodeDependencyReadynessCache = NewNodeDependencyReadynessCache()

	logger, err := l.NewLoggerFromEnvConfig(&envConfig, "unittest")
	assert.Nil(t, err)
	logger.PushF("TestIdxSeriousError")
	defer logger.PopF()

	mqProducer := mq.TestInmemProducer{}

	gocqlmemSession, cassandraEngineType, err := db.NewSession(&envConfig, ks, db.CreateKeyspaceOnConnect)
	assert.Nil(t, err)

	_, err = StartRun(&envConfig, logger, &mqProducer, "/tmp/capi_cfg/lookup_quicktest/script_quick.yaml", "/tmp/capi_cfg/lookup_quicktest/script_params_quick_fs_one.yaml", gocqlmemSession, cassandraEngineType, ks, []string{"read_orders", "read_order_items"}, "test run")
	assert.Nil(t, err)

	var runStatus wfmodel.RunStatusType
	runHistory, err := GetRunHistory(gocqlmemSession, ks)
	assert.Nil(t, err)

	runStatus = runHistory[len(runHistory)-1].Status
	logger.Info("TestRun.RUNSTATUS: %s", runStatus.ToString())
	assert.Equal(t, wfmodel.RunStart, runStatus)

	//var nodeRunStatus string
	for {
		msg := mqProducer.PeekHead()
		if msg == nil {
			break
		}
		queryPerformer := TableInserterQueryPerformerTestIdxNotAppliedSamePresentSecondRetry{}
		ackCmd := ProcessDataBatchMsg(&envConfig, logger, msg, 0, nil, ctx.TableInserterProperties{
			QueryPerformer:               &queryPerformer,
			DoesNotExistPauseMillis:      10, // speed it up for testing
			OperationTimedOutPauseMillis: 10, // speed it up for testing
		})
		if ackCmd == mq.AcknowledgerCmdAck {
			mqProducer.RemoveHead()
		} else {
			mqProducer.MoveHeadToTail()
		}
	}

	// Verify run status
	runHistory, err = GetRunHistory(gocqlmemSession, ks)
	assert.Nil(t, err)
	runStatus = runHistory[len(runHistory)-1].Status
	assert.Equal(t, wfmodel.RunComplete, runStatus)

	err = compareCsvs("/tmp/capi_out/lookup_quicktest/order_date_value_grouped_inner_baseline.csv", "/tmp/capi_out/lookup_quicktest/order_date_value_grouped_inner.csv")
	assert.Nil(t, err)
	err = compareCsvs("/tmp/capi_out/lookup_quicktest/order_date_value_grouped_left_outer_baseline.csv", "/tmp/capi_out/lookup_quicktest/order_date_value_grouped_left_outer.csv")
	assert.Nil(t, err)
	err = compareCsvs("/tmp/capi_out/lookup_quicktest/order_item_date_inner_baseline.csv", "/tmp/capi_out/lookup_quicktest/order_item_date_inner.csv")
	assert.Nil(t, err)
	err = compareCsvs("/tmp/capi_out/lookup_quicktest/order_item_date_left_outer_baseline.csv", "/tmp/capi_out/lookup_quicktest/order_item_date_left_outer.csv")
	assert.Nil(t, err)

	gocqlmemSession.Query(fmt.Sprintf("DROP keyspace %s;", ks)).Exec()
	gocqlmemSession.Close()
}

// 	} else if CurrentTestScenario == TestIdxNotAppliedDiffPresent {
// 		// UI: cannot write duplicate index key ... with ... on retry 0, existing record [...], rowid is different
// 		// give up immediately and report failure
// 		isApplied = false
// 		existingIdxRow["key"] = idxKey
// 		existingIdxRow["rowid"] = curRowid + 1

// TestIdxNotAppliedDiffPresent

type TableInserterQueryPerformerTestIdxNotAppliedDiffPresent struct {
	TotalIdxHits           int
	SimulatedIdxErrorCount int
}

func (qp *TableInserterQueryPerformerTestIdxNotAppliedDiffPresent) PerformInsertDataRecordWithRowid(gocqlSession gocqlshims.Session, pq *cql.PreparedQuery, preparedDataQueryParams []any, retryCount int) (map[string]any, bool, error) {
	return db.HelperPerformInsertDataRecordWithRowid(gocqlSession, pq, preparedDataQueryParams, retryCount)
}

func (qp *TableInserterQueryPerformerTestIdxNotAppliedDiffPresent) PerformInsertIdxRecordWithRowid(idxUniqueness sc.IdxUniqueness, gocqlSession gocqlshims.Session, pq *cql.PreparedQuery, preparedIdxQueryParams []any, retryCount int) (map[string]any, int, bool, error) {
	qp.TotalIdxHits++
	if retryCount > 0 || qp.SimulatedIdxErrorCount >= 1 {
		return db.HelperPerformInsertIdxRecordWithRowid(idxUniqueness, gocqlSession, pq, preparedIdxQueryParams, retryCount)
	} else {
		qp.SimulatedIdxErrorCount++
		// log: cannot write duplicate index key [%s] with %s,%d on retry %d, existing record [%v], rowid is different, throwing error %w
		// give up immediately and report failure
		existingIdxRow := map[string]any{}
		existingIdxRow["key"] = pq.Qb.PreparedColumnData.Values[pq.Qb.PreparedColumnData.ColumnIdxMap["key"]]
		existingIdxRow["rowid"] = -1 // Pray it's different than the passed one
		return existingIdxRow, retryCount, false, nil
	}
}

func TestIdxNotAppliedDiffPresent(t *testing.T) {
	os.Remove("/tmp/capi_out/lookup_quicktest/order_date_value_grouped_inner.csv")
	os.Remove("/tmp/capi_out/lookup_quicktest/order_date_value_grouped_left_outer.csv")
	os.Remove("/tmp/capi_out/lookup_quicktest/order_item_date_inner.csv")
	os.Remove("/tmp/capi_out/lookup_quicktest/order_item_date_left_outer.csv")

	ks := "ks_idx_not_applied_diff_present"

	envConfig := env.EnvConfig{
		Cassandra:                         env.CassandraConfig{WriterWorkers: 1},
		Log:                               env.LogConfig{Level: "INFO"},
		CustomProcessorDefFactoryInstance: &TestProcessorDefFactory{},
		UseGocqlmem:                       true,
	}
	sc.ScriptDefCache = sc.NewScriptDefCache()
	NodeDependencyReadynessCache = NewNodeDependencyReadynessCache()

	logger, err := l.NewLoggerFromEnvConfig(&envConfig, "unittest")
	assert.Nil(t, err)
	logger.PushF("TestIdxSeriousError")
	defer logger.PopF()

	mqProducer := mq.TestInmemProducer{}

	gocqlmemSession, cassandraEngineType, err := db.NewSession(&envConfig, ks, db.CreateKeyspaceOnConnect)
	assert.Nil(t, err)

	_, err = StartRun(&envConfig, logger, &mqProducer, "/tmp/capi_cfg/lookup_quicktest/script_quick.yaml", "/tmp/capi_cfg/lookup_quicktest/script_params_quick_fs_one.yaml", gocqlmemSession, cassandraEngineType, ks, []string{"read_orders", "read_order_items"}, "test run")
	assert.Nil(t, err)

	var runStatus wfmodel.RunStatusType
	runHistory, err := GetRunHistory(gocqlmemSession, ks)
	assert.Nil(t, err)

	runStatus = runHistory[len(runHistory)-1].Status
	logger.Info("TestRun.RUNSTATUS: %s", runStatus.ToString())
	assert.Equal(t, wfmodel.RunStart, runStatus)

	//var nodeRunStatus string
	for {
		msg := mqProducer.PeekHead()
		if msg == nil {
			break
		}
		queryPerformer := TableInserterQueryPerformerTestIdxNotAppliedDiffPresent{}
		ackCmd := ProcessDataBatchMsg(&envConfig, logger, msg, 0, nil, ctx.TableInserterProperties{
			QueryPerformer:               &queryPerformer,
			DoesNotExistPauseMillis:      10, // speed it up for testing
			OperationTimedOutPauseMillis: 10, // speed it up for testing
		})
		if ackCmd == mq.AcknowledgerCmdAck {
			mqProducer.RemoveHead()
		} else {
			mqProducer.MoveHeadToTail()
		}
	}

	// Verify run status
	runHistory, err = GetRunHistory(gocqlmemSession, ks)
	assert.Nil(t, err)
	runStatus = runHistory[len(runHistory)-1].Status
	assert.Equal(t, wfmodel.RunComplete, runStatus)

	// Verify node statuses
	nodeHistory, err := GetNodeHistoryForRuns(gocqlmemSession, ks, []int16{int16(1)})
	assert.Nil(t, err)
	newNodeRunStatusMap := map[string]wfmodel.NodeBatchStatusType{}
	for _, nodeEvent := range nodeHistory {
		newNodeRunStatusMap[nodeEvent.ScriptNode] = nodeEvent.Status
	}
	logger.Info("TestRun.NODESTATUS final: %s", fmt.Sprintf("%v", newNodeRunStatusMap))

	// For each node, verify batch statuses
	for nodeName, nodeStatus := range newNodeRunStatusMap {
		assert.Equal(t, wfmodel.NodeBatchFail, nodeStatus, fmt.Sprintf("node %s supposed to fail", nodeName))
		// Make sure all batches for this node started then failed
		batchEvents, err := GetBatchHistoryForRunAndNode(gocqlmemSession, ks, int16(1), nodeName)
		assert.Nil(t, err)
		if nodeName == "read_orders" || nodeName == "read_order_items" {
			// These nodes start and fail
			assert.Equal(t, 2, len(batchEvents), nodeName)
			assert.Equal(t, wfmodel.NodeBatchStart, batchEvents[0].Status, nodeName)
			assert.Equal(t, wfmodel.NodeBatchFail, batchEvents[1].Status, nodeName)
			assert.True(t, strings.Contains(batchEvents[1].Comment, "rowid is different, throwing error duplicate key"))
		} else {
			// These nodes failed without starting
			for _, event := range batchEvents {
				assert.Equal(t, wfmodel.NodeBatchFail, event.Status, nodeName)
				assert.True(t, strings.Contains(event.Comment, "some dependency nodes"))
			}
		}
	}
	gocqlmemSession.Query(fmt.Sprintf("DROP keyspace %s;", ks)).Exec()
	gocqlmemSession.Close()
}
