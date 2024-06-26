package proc

import (
	"fmt"
	"io"
	"time"

	"github.com/capillariesio/capillaries/pkg/ctx"
	"github.com/capillariesio/capillaries/pkg/env"
	"github.com/capillariesio/capillaries/pkg/eval"
	"github.com/capillariesio/capillaries/pkg/l"
	"github.com/capillariesio/capillaries/pkg/sc"
	"github.com/capillariesio/capillaries/pkg/storage"
	gp "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
)

func readParquetRowToValuesMap(d map[string]any,
	rowIdx int,
	requestedParquetColumnNames []string,
	parquetToCapiFieldNameMap map[string]string,
	parquetToCapiTypeMap map[string]sc.TableFieldType,
	schemaElementMap map[string]*parquet.SchemaElement,
	colVars eval.VarValuesMap) error {
	colVars[sc.ReaderAlias] = map[string]any{}
	for _, parquetColName := range requestedParquetColumnNames {
		capiFieldName, ok := parquetToCapiFieldNameMap[parquetColName]
		if !ok {
			return fmt.Errorf("dev error, parquet column %s does not map to a Capillaries field", parquetColName)
		}

		capiType, ok := parquetToCapiTypeMap[parquetColName]
		if !ok {
			return fmt.Errorf("dev error, parquet column %s does not map to a Capillaries type", parquetColName)
		}

		se, ok := schemaElementMap[parquetColName]
		if !ok {
			return fmt.Errorf("dev error, no schema element found for parquet column %s", parquetColName)
		}

		volatile, present := d[parquetColName]
		if !present || volatile == nil {
			colVars[sc.ReaderAlias][capiFieldName] = sc.GetDefaultFieldTypeValue(capiType)
			continue
		}

		var err error
		switch capiType {
		case sc.FieldTypeString:
			if colVars[sc.ReaderAlias][capiFieldName], err = storage.ParquetReadString(volatile, se); err != nil {
				return fmt.Errorf("cannot read string row %d, column %s: %s", rowIdx, parquetColName, err.Error())
			}
		case sc.FieldTypeInt:
			if colVars[sc.ReaderAlias][capiFieldName], err = storage.ParquetReadInt(volatile, se); err != nil {
				return fmt.Errorf("cannot read int row %d, column %s: %s", rowIdx, parquetColName, err.Error())
			}
		case sc.FieldTypeFloat:
			if colVars[sc.ReaderAlias][capiFieldName], err = storage.ParquetReadFloat(volatile, se); err != nil {
				return fmt.Errorf("cannot read float row %d, column %s: %s", rowIdx, parquetColName, err.Error())
			}
		case sc.FieldTypeBool:
			if colVars[sc.ReaderAlias][capiFieldName], err = storage.ParquetReadBool(volatile, se); err != nil {
				return fmt.Errorf("cannot read bool row %d, column %s: %s", rowIdx, parquetColName, err.Error())
			}
		case sc.FieldTypeDateTime:
			if colVars[sc.ReaderAlias][capiFieldName], err = storage.ParquetReadDateTime(volatile, se); err != nil {
				return fmt.Errorf("cannot read DateTime row %d, column %s: %s", rowIdx, parquetColName, err.Error())
			}
		case sc.FieldTypeDecimal2:
			if colVars[sc.ReaderAlias][capiFieldName], err = storage.ParquetReadDecimal2(volatile, se); err != nil {
				return fmt.Errorf("cannot read decimal2 row %d, column %s: %s", rowIdx, parquetColName, err.Error())
			}
		default:
			return fmt.Errorf("cannot read unsupported type %s row %d, column %s", capiType, rowIdx, parquetColName)
		}
	}

	return nil
}

func readParquet(envConfig *env.EnvConfig, logger *l.CapiLogger, pCtx *ctx.MessageProcessingContext, totalStartTime time.Time, filePath string, fileReadSeeker io.ReadSeeker) (BatchStats, error) {
	node := pCtx.CurrentScriptNode
	bs := BatchStats{RowsRead: 0, RowsWritten: 0, Src: filePath, Dst: node.TableCreator.Name}

	if fileReadSeeker == nil {
		return bs, fmt.Errorf("cannot read parquet file without io.ReadSeeker: %s", filePath)
	}

	// Digest source column config from the script
	requestedParquetColumnNames := make([]string, len(node.FileReader.Columns))
	parquetToCapiFieldNameMap := map[string]string{}
	colIdx := 0
	for capiFieldName, colDef := range node.FileReader.Columns {
		requestedParquetColumnNames[colIdx] = colDef.Parquet.SrcColName
		parquetToCapiFieldNameMap[colDef.Parquet.SrcColName] = capiFieldName
		colIdx++
	}

	reader, err := gp.NewFileReader(fileReadSeeker, requestedParquetColumnNames...)
	if err != nil {
		return bs, err
	}

	// Digest schema
	schemaElementMap := map[string]*parquet.SchemaElement{}
	parquetToCapiTypeMap := map[string]sc.TableFieldType{}
	schemaDef := reader.GetSchemaDefinition()
	for _, column := range schemaDef.RootColumn.Children {
		t, err := storage.ParquetGuessCapiType(column.SchemaElement)
		if err != nil {
			return bs, fmt.Errorf("cannot read parquet column %s: %s", column.SchemaElement.Name, err.Error())
		}
		parquetToCapiTypeMap[column.SchemaElement.Name] = t
		schemaElementMap[column.SchemaElement.Name] = column.SchemaElement
	}

	// Check that for each Parquet field we have a corresponding mapping
	for _, requestedColumnName := range requestedParquetColumnNames {
		if _, ok := schemaElementMap[requestedColumnName]; !ok {
			return bs, fmt.Errorf("cannot find requested parquet column in the file: %s", requestedColumnName)
		}
	}

	lineIdx := int64(0)
	// tableRecordBatchCount := 0

	// Prepare inserter
	instr, err := createInserterAndStartWorkers(logger, envConfig, pCtx, &node.TableCreator, DefaultInserterBatchSize, DataIdxSeqModeDataFirst, logger.ZapMachine.String)
	if err != nil {
		return bs, err
	}
	defer instr.letWorkersDrainRecordWrittenStatusesAndCloseInserter(logger, pCtx)

	// batchStartTime := time.Now()
	for {
		d, err := reader.NextRow()

		if err == io.EOF {
			break
		}
		if err != nil {
			return bs, fmt.Errorf("cannot get row %d: %s", bs.RowsRead, err.Error())
		}

		colVars := eval.VarValuesMap{}
		if err := readParquetRowToValuesMap(d, bs.RowsRead, requestedParquetColumnNames, parquetToCapiFieldNameMap, parquetToCapiTypeMap, schemaElementMap, colVars); err != nil {
			return bs, err
		}

		// TableCreator: evaluate table column expressions
		tableRecord, err := node.TableCreator.CalculateTableRecordFromSrcVars(false, colVars)
		if err != nil {
			return bs, fmt.Errorf("cannot populate table record from [%s], line %d: [%s]", filePath, lineIdx, err.Error())
		}

		// Check table creator having
		inResult, err := node.TableCreator.CheckTableRecordHavingCondition(tableRecord)
		if err != nil {
			return bs, fmt.Errorf("cannot check having condition [%s], table record [%v]: [%s]", node.TableCreator.RawHaving, tableRecord, err.Error())
		}

		// Write batch if needed
		if inResult {
			indexKeyMap, err := instr.buildIndexKeys(tableRecord)
			if err != nil {
				return bs, fmt.Errorf("cannot build index keys for %s: [%s]", node.TableCreator.Name, err.Error())
			}

			if len(instr.RecordWrittenStatuses) == cap(instr.RecordWrittenStatuses) {
				if err := instr.letWorkersDrainRecordWrittenStatuses(logger, pCtx); err != nil {
					return bs, err
				}
			}
			if err := instr.add(logger, pCtx, tableRecord, indexKeyMap); err != nil {
				return bs, fmt.Errorf("cannot add record to inserter %s: [%s]", node.TableCreator.Name, err.Error())
			}

			bs.RowsWritten++
		}
		bs.RowsRead++
	}

	// Write leftovers if anything was sent at all
	if instr.RecordsSent > 0 {
		if err := instr.letWorkersDrainRecordWrittenStatuses(logger, pCtx); err != nil {
			return bs, err
		}
	}

	// reportWriteTableLeftovers(logger, pCtx, tableRecordBatchCount, time.Since(batchStartTime), len(node.TableCreator.Indexes), instr.NumWorkers)

	bs.Elapsed = time.Since(totalStartTime)
	reportWriteTableComplete(logger, pCtx, bs.RowsRead, bs.RowsWritten, bs.Elapsed, len(node.TableCreator.Indexes), instr.NumWorkers)

	return bs, nil
}
