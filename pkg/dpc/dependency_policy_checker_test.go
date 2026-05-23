package dpc

import (
	"regexp"
	"testing"

	"github.com/capillariesio/capillaries/pkg/sc"
	"github.com/capillariesio/capillaries/pkg/wfmodel"
	"github.com/stretchr/testify/assert"
)

func TestDefaultDependencyPolicyChecker(t *testing.T) {
	nrsSlice := wfmodel.DependencyNodeRunStatusSlice{
		{
			RunId:        10,
			RunIsCurrent: true,
			RunStatus:    wfmodel.RunStart,
			NodeStatus:   wfmodel.NodeBatchNone,
		},
	}

	polDef := sc.DependencyPolicyDef{}
	if err := polDef.Deserialize([]byte(sc.DefaultPolicyCheckerConfJson), sc.ScriptJson); err != nil {
		t.Error(err)
		return
	}

	var cmd sc.ReadyToRunNodeCmdType
	var runId int16
	var matchedRuleIdx int
	var err error
	fullBatchId := "some_node"

	nrsSlice[0].RunIsCurrent = true

	// Run is started, but node already says stop received, wait for this run to be marked as stopped
	nrsSlice[0].NodeStatus = wfmodel.NodeBatchRunStopReceived
	cmd, _, matchedRuleIdx, err = CheckDependencyPolicyAgainstNodeEventList(nil, fullBatchId, &polDef, nrsSlice)
	assert.Nil(t, err)
	assert.Equal(t, sc.NodeWait, cmd)
	assert.Equal(t, -1, matchedRuleIdx) // "no rules matched against events (wait)"

	nrsSlice[0].NodeStatus = wfmodel.NodeBatchSuccess
	cmd, runId, matchedRuleIdx, err = CheckDependencyPolicyAgainstNodeEventList(nil, fullBatchId, &polDef, nrsSlice)
	assert.Nil(t, err)
	assert.Equal(t, sc.NodeGo, cmd)
	assert.Equal(t, int16(10), runId)
	assert.Equal(t, 0, matchedRuleIdx) // "matched rule 0(go)"

	nrsSlice[0].NodeStatus = wfmodel.NodeBatchNone
	cmd, _, matchedRuleIdx, err = CheckDependencyPolicyAgainstNodeEventList(nil, fullBatchId, &polDef, nrsSlice)
	assert.Nil(t, err)
	assert.Equal(t, sc.NodeWait, cmd)
	assert.Equal(t, 1, matchedRuleIdx) // "matched rule 1(wait)"

	nrsSlice[0].NodeStatus = wfmodel.NodeBatchStart
	cmd, _, matchedRuleIdx, err = CheckDependencyPolicyAgainstNodeEventList(nil, fullBatchId, &polDef, nrsSlice)
	assert.Nil(t, err)
	assert.Equal(t, sc.NodeWait, cmd)
	assert.Equal(t, 2, matchedRuleIdx) // "matched rule 2(wait)"

	nrsSlice[0].NodeStatus = wfmodel.NodeBatchFail
	cmd, _, matchedRuleIdx, err = CheckDependencyPolicyAgainstNodeEventList(nil, fullBatchId, &polDef, nrsSlice)
	assert.Nil(t, err)
	assert.Equal(t, sc.NodeNogo, cmd)
	assert.Equal(t, 3, matchedRuleIdx) // "matched rule 3(nogo)"

	nrsSlice[0].RunIsCurrent = false

	// Previous run is started, but node already says stop received, wait for previous run to be marked as stopped
	nrsSlice[0].NodeStatus = wfmodel.NodeBatchRunStopReceived
	cmd, _, matchedRuleIdx, err = CheckDependencyPolicyAgainstNodeEventList(nil, fullBatchId, &polDef, nrsSlice)
	assert.Nil(t, err)
	assert.Equal(t, sc.NodeWait, cmd)
	assert.Equal(t, -1, matchedRuleIdx) // "no rules matched against events (wait)"

	nrsSlice[0].NodeStatus = wfmodel.NodeBatchSuccess
	cmd, runId, matchedRuleIdx, err = CheckDependencyPolicyAgainstNodeEventList(nil, fullBatchId, &polDef, nrsSlice)
	assert.Nil(t, err)
	assert.Equal(t, sc.NodeGo, cmd)
	assert.Equal(t, int16(10), runId)
	assert.Equal(t, 4, matchedRuleIdx) // "matched rule 4(go)"

	nrsSlice[0].NodeStatus = wfmodel.NodeBatchNone
	cmd, _, matchedRuleIdx, err = CheckDependencyPolicyAgainstNodeEventList(nil, fullBatchId, &polDef, nrsSlice)
	assert.Nil(t, err)
	assert.Equal(t, sc.NodeWait, cmd)
	assert.Equal(t, 5, matchedRuleIdx) // "matched rule 5(wait)"

	nrsSlice[0].NodeStatus = wfmodel.NodeBatchStart
	cmd, _, matchedRuleIdx, err = CheckDependencyPolicyAgainstNodeEventList(nil, fullBatchId, &polDef, nrsSlice)
	assert.Nil(t, err)
	assert.Equal(t, sc.NodeWait, cmd)
	assert.Equal(t, 6, matchedRuleIdx) // "matched rule 6(wait)"

	// Previous run completed
	nrsSlice[0].RunStatus = wfmodel.RunComplete

	nrsSlice[0].NodeStatus = wfmodel.NodeBatchSuccess
	cmd, runId, matchedRuleIdx, err = CheckDependencyPolicyAgainstNodeEventList(nil, fullBatchId, &polDef, nrsSlice)
	assert.Nil(t, err)
	assert.Equal(t, sc.NodeGo, cmd)
	assert.Equal(t, int16(10), runId)
	assert.Equal(t, 7, matchedRuleIdx) // "matched rule 7(go)"

	nrsSlice[0].NodeStatus = wfmodel.NodeBatchFail
	cmd, _, matchedRuleIdx, err = CheckDependencyPolicyAgainstNodeEventList(nil, fullBatchId, &polDef, nrsSlice)
	assert.Nil(t, err)
	assert.Equal(t, sc.NodeNogo, cmd)
	assert.Equal(t, 8, matchedRuleIdx) // "matched rule 8(nogo)"

	// Run complete, but batch still running, assume Cassandra is not coherent yet
	nrsSlice[0].NodeStatus = wfmodel.NodeBatchStart
	cmd, _, matchedRuleIdx, err = CheckDependencyPolicyAgainstNodeEventList(nil, fullBatchId, &polDef, nrsSlice)
	assert.Nil(t, err)
	assert.Equal(t, sc.NodeWait, cmd)
	assert.Equal(t, -1, matchedRuleIdx) // "no rules matched against events (wait)"

	// Run complete, but node never started, should never end here,
	// this means Cassandra node/run state incoherence was there for very long (more than a couple seconds)
	nrsSlice[0].NodeStatus = wfmodel.NodeBatchNone
	cmd, _, matchedRuleIdx, err = CheckDependencyPolicyAgainstNodeEventList(nil, fullBatchId, &polDef, nrsSlice)
	assert.Nil(t, err)
	assert.Equal(t, sc.NodeWait, cmd)
	assert.Equal(t, -1, matchedRuleIdx) // "no rules matched against events (wait)"

	// Failures

	re := regexp.MustCompile(`"expression": "nrs\.run[^"]+"`)
	err = polDef.Deserialize([]byte(re.ReplaceAllString(sc.DefaultPolicyCheckerConfJson, `"expression": "1"`)), sc.ScriptJson)
	assert.Nil(t, err)
	_, _, _, err = CheckDependencyPolicyAgainstNodeEventList(nil, fullBatchId, &polDef, nrsSlice)
	assert.Contains(t, err.Error(), "expected result type was bool, got int64")
}
