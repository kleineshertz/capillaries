package wfdb

import (
	"github.com/capillariesio/capillaries/pkg/ctx"
	"github.com/capillariesio/capillaries/pkg/wfmodel"
)

func BuildDependencyNodeRunStatusMap(pCtx *ctx.MessageProcessingContext, depNodeNames []string) (map[string][]wfmodel.DependencyNodeRunStatus, error) {
	// All runs in this ks with their properties
	runPropertiesFields := []string{"run_id", "affected_nodes"}
	rows, err := GetAllRunsProperties(pCtx.CqlSession, pCtx.Msg.DataKeyspace, runPropertiesFields)
	if err != nil {
		return nil, err
	}

	// Say, for current run 4 we may get [run1, run2, run3 ] and { run1: ["nodeReader"], run2: ["nodeLookup"], run3: ["nodeLookup"] }
	depRunIds, depeRunNodesMap, err := wfmodel.MultipleRunsPropertiesToDependencies(rows, depNodeNames, runPropertiesFields)
	if err != nil {
		return nil, err
	}

	// Run history only for "dependency" runs
	rows, err = GetRunHistory(pCtx.CqlSession, pCtx.Msg.DataKeyspace, depRunIds)
	if err != nil {
		return nil, err
	}

	sortedRunHistoryEvents, err := wfmodel.RunHistoryRowsToEvents(rows)
	if err != nil {
		return nil, err
	}

	// Get { run1: complete, run2: stopped, run3: complete }
	runStatusMap := wfmodel.RunHistoryEventsToRunStatusMap(sortedRunHistoryEvents)

	rows, err = GetNodeHistoryForRuns(pCtx.CqlSession, pCtx.Msg.DataKeyspace, depRunIds, depNodeNames)
	if err != nil {
		return nil, err
	}

	sortedNodeEvents, err := wfmodel.NodeHistoryRowsToEvents(rows)
	if err != nil {
		return nil, err
	}

	// Build { nodeReader: [{run1, RunComplete, NodeSuccess}], nodeLookup: [{run2, RunStopped, NodeSuccess}, {run3, RunComplete, NodeSuccess}]  }
	resultMap := map[string][]wfmodel.DependencyNodeRunStatus{}
	for _, runId := range depRunIds {
		_, nodeStatusMap := wfmodel.FigureOutRunStatusAndAffectedNodesStatusesFromNodeEvents(sortedNodeEvents, runId, depeRunNodesMap[runId])
		for nodeName, nodeStatus := range nodeStatusMap {
			nrs := wfmodel.DependencyNodeRunStatus{
				RunId:        runId,
				RunIsCurrent: runId == pCtx.Msg.RunId,
				RunStatus:    runStatusMap[runId],
				NodeStatus:   nodeStatus,
			}
			if _, ok := resultMap[nodeName]; !ok {
				resultMap[nodeName] = make([]wfmodel.DependencyNodeRunStatus, 0)
			}
			resultMap[nodeName] = append(resultMap[nodeName], nrs)
		}
	}
	return resultMap, nil
}
