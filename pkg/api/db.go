package api

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/capillariesio/capillaries/pkg/cql"
	"github.com/capillariesio/capillaries/pkg/db"
	"github.com/capillariesio/capillaries/pkg/l"
	"github.com/capillariesio/capillaries/pkg/proc"
	"github.com/capillariesio/capillaries/pkg/sc"
	"github.com/capillariesio/capillaries/pkg/wfdb"
	"github.com/capillariesio/capillaries/pkg/wfmodel"
	"github.com/gocql/gocql"
)

const ProhibitedKeyspaceNameRegex = "^system"
const AllowedKeyspaceNameRegex = "[a-zA-Z0-9_]+"

// Used by Webapi to ignore Cassandra system keyspaces
func IsSystemKeyspaceName(keyspace string) bool {
	re := regexp.MustCompile(ProhibitedKeyspaceNameRegex)
	invalidNamePieceFound := re.FindString(keyspace)
	return len(invalidNamePieceFound) > 0
}

func checkKeyspaceName(keyspace string) error {
	re := regexp.MustCompile(ProhibitedKeyspaceNameRegex)
	invalidNamePieceFound := re.FindString(keyspace)
	if len(invalidNamePieceFound) > 0 {
		return fmt.Errorf("invalid keyspace name [%s]: prohibited regex is [%s]", keyspace, ProhibitedKeyspaceNameRegex)
	}
	re = regexp.MustCompile(AllowedKeyspaceNameRegex)
	if !re.MatchString(keyspace) {
		return fmt.Errorf("invalid keyspace name [%s]: allowed regex is [%s]", keyspace, AllowedKeyspaceNameRegex)
	}
	return nil
}

// Used by Toolbelt get_table_cql cmd, prints out CQL to create all workflow tables and data/index tables for a specific keyspace.
// startNodeNames - list of script nodes that will be started immediately upon run start,
// helps find out which tables referenced by the script will be affected, to avoid unnecessary table creation.
func GetTablesCql(script *sc.ScriptDef, keyspace string, runId int16, startNodeNames []string) string {
	sb := strings.Builder{}
	sb.WriteString("-- Workflow\n")
	sb.WriteString(fmt.Sprintf("%s\n", wfmodel.GetCreateTableCql(reflect.TypeOf(wfmodel.BatchHistoryEvent{}), keyspace, wfmodel.TableNameBatchHistory)))
	sb.WriteString(fmt.Sprintf("%s\n", wfmodel.GetCreateTableCql(reflect.TypeOf(wfmodel.NodeHistoryEvent{}), keyspace, wfmodel.TableNameNodeHistory)))
	sb.WriteString(fmt.Sprintf("%s\n", wfmodel.GetCreateTableCql(reflect.TypeOf(wfmodel.RunHistoryEvent{}), keyspace, wfmodel.TableNameRunHistory)))
	sb.WriteString(fmt.Sprintf("%s\n", wfmodel.GetCreateTableCql(reflect.TypeOf(wfmodel.RunProperties{}), keyspace, wfmodel.TableNameRunAffectedNodes)))
	sb.WriteString(fmt.Sprintf("%s\n", wfmodel.GetCreateTableCql(reflect.TypeOf(wfmodel.RunCounter{}), keyspace, wfmodel.TableNameRunCounter)))
	qb := cql.QueryBuilder{}
	sb.WriteString(fmt.Sprintf("%s\n", qb.Keyspace(keyspace).Write("ks", keyspace).Write("last_run", 0).InsertUnpreparedQuery(wfmodel.TableNameRunCounter, cql.IgnoreIfExists)))

	for _, nodeName := range script.GetAffectedNodes(startNodeNames) {
		node, ok := script.ScriptNodes[nodeName]
		if !ok || !node.HasTableCreator() {
			continue
		}
		sb.WriteString(fmt.Sprintf("-- %s\n", nodeName))
		sb.WriteString(fmt.Sprintf("%s\n", proc.CreateDataTableCql(keyspace, runId, &node.TableCreator)))
		for idxName, idxDef := range node.TableCreator.Indexes {
			sb.WriteString(fmt.Sprintf("%s\n", proc.CreateIdxTableCql(keyspace, runId, idxName, idxDef, &node.TableCreator)))
		}
	}
	return sb.String()
}

// Used by Toolbelt and Webapi to drop Cassandra keyspace
func DropKeyspace(logger *l.CapiLogger, cqlSession *gocql.Session, keyspace string) error {
	logger.PushF("api.DropKeyspace")
	defer logger.PopF()

	dbStartTime := time.Now()

	if err := checkKeyspaceName(keyspace); err != nil {
		return err
	}

	qb := cql.QueryBuilder{}
	q := qb.
		Keyspace(keyspace).
		DropKeyspace()
	if err := cqlSession.Query(q).Exec(); err != nil {
		return db.WrapDbErrorWithQuery("cannot drop keyspace", q, err)
	}

	if err := db.VerifyKeyspaceDeleted(cqlSession, keyspace); err != nil {
		return err
	}

	logger.Info("drop keyspace %s took %.2fs", keyspace, time.Since(dbStartTime).Seconds())

	return nil
}

// Used by Webapi to retrieve all runs that happened in this keyspace and their current status
func HarvestRunLifespans(logger *l.CapiLogger, cqlSession *gocql.Session, keyspace string, runIds []int16) (wfmodel.RunLifespanMap, error) {
	logger.PushF("api.HarvestRunLifespans")
	defer logger.PopF()

	return wfdb.HarvestRunLifespans(logger, cqlSession, keyspace, runIds)
}

// Used by Webapi to retrieve static run properties
func GetRunProperties(logger *l.CapiLogger, cqlSession *gocql.Session, keyspace string, runId int16) ([]*wfmodel.RunProperties, error) {
	logger.PushF("api.GetRunProperties")
	defer logger.PopF()
	return wfdb.GetRunProperties(logger, cqlSession, keyspace, runId)
}

// Used by Webapi to retrieve each node status history for a run
func GetNodeHistoryForRun(logger *l.CapiLogger, cqlSession *gocql.Session, keyspace string, runId int16) ([]*wfmodel.NodeHistoryEvent, error) {
	logger.PushF("api.GetNodeHistoryForRun")
	defer logger.PopF()

	return wfdb.GetNodeHistoryForRun(logger, cqlSession, keyspace, runId)
}

// Used by Webapi to retrieve batch status history for a run/node pair
func GetRunNodeBatchHistory(logger *l.CapiLogger, cqlSession *gocql.Session, keyspace string, runId int16, nodeName string) ([]*wfmodel.BatchHistoryEvent, error) {
	logger.PushF("api.GetRunNodeBatchHistory")
	defer logger.PopF()
	return wfdb.GetRunNodeBatchHistory(logger, cqlSession, keyspace, runId, nodeName)
}
