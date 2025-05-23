package proc

import (
	"container/heap"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"time"

	"github.com/capillariesio/capillaries/pkg/cql"
	"github.com/capillariesio/capillaries/pkg/ctx"
	"github.com/capillariesio/capillaries/pkg/env"
	"github.com/capillariesio/capillaries/pkg/eval"
	"github.com/capillariesio/capillaries/pkg/l"
	"github.com/capillariesio/capillaries/pkg/sc"
	"github.com/capillariesio/capillaries/pkg/xfer"
)

type FileRecordHeapItem struct {
	FileRecord *[]any
	Key        string
}

type FileRecordHeap []*FileRecordHeapItem

func (h FileRecordHeap) Len() int           { return len(h) }
func (h FileRecordHeap) Less(i, j int) bool { return h[i].Key > h[j].Key } // Reverse order: https://stackoverflow.com/questions/49065781/limit-size-of-the-priority-queue-for-gos-heap-interface-implementation
func (h FileRecordHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *FileRecordHeap) Push(x any) {
	item := x.(*FileRecordHeapItem)
	*h = append(*h, item)
}
func (h *FileRecordHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*h = old[0 : n-1]
	return item
}

func readAndInsert(logger *l.CapiLogger, pCtx *ctx.MessageProcessingContext, tableName string, rs *Rowset, instr *FileInserter, readerNodeRunId int16, startToken int64, endToken int64, srcBatchSize int) (BatchStats, error) {

	bs := BatchStats{RowsRead: 0, RowsWritten: 0, Src: tableName + cql.RunIdSuffix(readerNodeRunId), Dst: instr.FinalFileUrl}

	var topHeap FileRecordHeap
	if instr.FileCreator.HasTop() {
		topHeap := FileRecordHeap{}
		heap.Init(&topHeap)
	}

	curStartToken := startToken

	for {
		lastRetrievedToken, err := selectBatchFromTableByToken(logger,
			pCtx,
			rs,
			tableName,
			readerNodeRunId,
			srcBatchSize,
			curStartToken,
			endToken)
		if err != nil {
			return bs, err
		}
		curStartToken = lastRetrievedToken + 1

		if rs.RowCount == 0 {
			break
		}

		for rowIdx := 0; rowIdx < rs.RowCount; rowIdx++ {
			vars := eval.VarValuesMap{}
			if err := rs.ExportToVars(rowIdx, &vars); err != nil {
				return bs, err
			}

			fileRecord, err := instr.FileCreator.CalculateFileRecordFromSrcVars(vars)
			if err != nil {
				return bs, fmt.Errorf("cannot populate file record from [%v]: [%s]", vars, err.Error())
			}

			inResult, err := instr.FileCreator.CheckFileRecordHavingCondition(fileRecord)
			if err != nil {
				return bs, fmt.Errorf("cannot check having condition [%s], file record [%v]: [%s]", instr.FileCreator.RawHaving, fileRecord, err.Error())
			}

			if !inResult {
				continue
			}

			if instr.FileCreator.HasTop() {
				keyVars := map[string]any{}
				for i := 0; i < len(instr.FileCreator.Columns); i++ {
					keyVars[instr.FileCreator.Columns[i].Name] = fileRecord[i]
				}
				key, err := sc.BuildKey(keyVars, &instr.FileCreator.Top.OrderIdxDef)
				if err != nil {
					return bs, fmt.Errorf("cannot build top key for [%v]: [%s]", vars, err.Error())
				}
				heap.Push(&topHeap, &FileRecordHeapItem{FileRecord: &fileRecord, Key: key})
				if len(topHeap) > instr.FileCreator.Top.Limit {
					heap.Pop(&topHeap)
				}
			} else {
				instr.add(fileRecord)
				bs.RowsWritten++
			}
		}

		bs.RowsRead += rs.RowCount
		if rs.RowCount < srcBatchSize {
			break
		}

		if err := instr.checkWorkerOutputForErrors(); err != nil {
			return bs, fmt.Errorf("cannot save record batch from %s to %s(temp %s): [%s]", tableName, instr.FinalFileUrl, instr.TempFilePath, err.Error())
		}

	} // for each source table batch

	if instr.FileCreator.HasTop() {
		properlyOrderedTopList := make([]*FileRecordHeapItem, topHeap.Len())
		for i := topHeap.Len() - 1; i >= 0; i-- {
			properlyOrderedTopList[i] = heap.Pop(&topHeap).(*FileRecordHeapItem)
		}
		for i := 0; i < len(properlyOrderedTopList); i++ {
			instr.add(*properlyOrderedTopList[i].FileRecord)
			bs.RowsWritten++
		}
	}

	return bs, nil

}

func RunCreateFile(envConfig *env.EnvConfig,
	logger *l.CapiLogger,
	pCtx *ctx.MessageProcessingContext,
	readerNodeRunId int16,
	startToken int64,
	endToken int64) (BatchStats, error) {

	logger.PushF("proc.RunCreateFile")
	defer logger.PopF()

	totalStartTime := time.Now()

	if readerNodeRunId == 0 {
		return BatchStats{RowsRead: 0, RowsWritten: 0}, errors.New("this node has a dependency node to read data from that was never started in this keyspace (readerNodeRunId == 0)")
	}

	node := pCtx.CurrentScriptNode

	if !node.HasFileCreator() {
		return BatchStats{RowsRead: 0, RowsWritten: 0}, errors.New("node does not have file creator")
	}

	// Fields to read from source table
	srcFieldRefs := sc.FieldRefs{}
	// No src fields in having!
	srcFieldRefs.AppendWithFilter(node.FileCreator.UsedInTargetExpressionsFields, sc.ReaderAlias)

	rs := NewRowsetFromFieldRefs(
		sc.FieldRefs{sc.RowidFieldRef(node.TableReader.TableName)},
		sc.FieldRefs{sc.RowidTokenFieldRef()},
		srcFieldRefs)

	instr := newFileInserter(pCtx, &node.FileCreator, pCtx.BatchInfo.RunId, pCtx.BatchInfo.BatchIdx)

	u, err := url.Parse(instr.FinalFileUrl)
	if err != nil {
		return BatchStats{RowsRead: 0, RowsWritten: 0}, fmt.Errorf("cannot parse file url %s: %s", instr.FinalFileUrl, err.Error())
	}

	if node.FileCreator.CreatorFileType == sc.CreatorFileTypeCsv {
		if err := instr.createCsvFileAndStartWorker(logger, u); err != nil {
			return BatchStats{RowsRead: 0, RowsWritten: 0}, fmt.Errorf("cannot start csv inserter worker: %s", err.Error())
		}
	} else if node.FileCreator.CreatorFileType == sc.CreatorFileTypeParquet {
		if err := instr.createParquetFileAndStartWorker(logger, node.FileCreator.Parquet.Codec, u); err != nil {
			return BatchStats{RowsRead: 0, RowsWritten: 0}, fmt.Errorf("cannot start parquet inserter worker: %s", err.Error())
		}
	} else {
		return BatchStats{RowsRead: 0, RowsWritten: 0}, fmt.Errorf("unknown inserter file type: %d", node.FileCreator.CreatorFileType)
	}

	bs, err := readAndInsert(logger, pCtx, node.TableReader.TableName, rs, instr, readerNodeRunId, startToken, endToken, node.TableReader.RowsetSize)
	if err != nil {
		if closeErr := instr.waitForWorkerAndCloseErrorsOut(logger, pCtx); err != nil {
			logger.ErrorCtx(pCtx, "unexpected error while calling waitForWorkerAndCloseErrorsOut: %s", closeErr.Error())
		}
		return bs, err
	}

	// Successful so far, write leftovers
	if err := instr.waitForWorkerAndCloseErrorsOut(logger, pCtx); err != nil {
		return bs, fmt.Errorf("cannot save record batch from %s to %s(temp %s): [%s]", node.TableReader.TableName, instr.FinalFileUrl, instr.TempFilePath, err.Error())
	}

	bs.Elapsed = time.Since(totalStartTime)
	logger.InfoCtx(pCtx, "WriteFileComplete: read %d, wrote %d items in %.3fs (%.0f items/s)", bs.RowsRead, bs.RowsWritten, bs.Elapsed.Seconds(), float64(bs.RowsWritten)/bs.Elapsed.Seconds())

	if instr.TempFilePath == "" {
		// Nothing to do, the file is already at its destination
		return bs, nil
	}
	defer os.Remove(instr.TempFilePath)

	// TODO: make it prettier
	// Wait till inserter calls w.Close() to flush the file
	var st fs.FileInfo
	for i := 0; i < 30; i++ {
		st, err = os.Stat(instr.TempFilePath)
		if err != nil {
			return bs, fmt.Errorf("cannot get size of result file %s: %s", instr.TempFilePath, err.Error())
		}
		if st.Size() > 0 {
			break
		}
		time.Sleep(1 * time.Second)
	}

	if st.Size() == 0 {
		return bs, fmt.Errorf("cannot obtain non-empty result file %s", instr.TempFilePath)
	}

	logger.InfoCtx(pCtx, "uploading %s of size %d to %s...", instr.TempFilePath, st.Size(), instr.FinalFileUrl)

	if u.Scheme == xfer.UrlSchemeSftp {
		return bs, xfer.UploadSftpFile(instr.TempFilePath, instr.FinalFileUrl, envConfig.PrivateKeys)
	} else if u.Scheme == xfer.UrlSchemeS3 {
		return bs, xfer.UploadS3File(instr.TempFilePath, u)
	}

	return bs, fmt.Errorf("unexpected URL scheme %s in %s", u.Scheme, instr.FinalFileUrl)
}
