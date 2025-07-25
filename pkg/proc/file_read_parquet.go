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

	// Prepare inserter
	instr, err := createInserterAndStartWorkers(logger, envConfig, pCtx, &node.TableCreator, DataIdxSeqModeDataFirst, logger.ZapMachine.String)
	if err != nil {
		return bs, err
	}
	instr.startDrainer()
	defer instr.closeInserter(logger, pCtx)

	// Minimize allocations to help GC in this high-traffic loop
	var tableRecord map[string]any
	indexKeyMap := map[string]string{}
	colVars := eval.VarValuesMap{}
	var d map[string]any
	var inResult bool
	for {
		d, err = reader.NextRow()

		if err == io.EOF {
			break
		}
		if err != nil {
			instr.cancelDrainer(fmt.Errorf("cannot get parquet [%s] row %d: %s", filePath, bs.RowsRead, err.Error()))
			return bs, instr.waitForDrainer(logger, pCtx)
		}

		clear(colVars)
		if err := readParquetRowToValuesMap(d, bs.RowsRead, requestedParquetColumnNames, parquetToCapiFieldNameMap, parquetToCapiTypeMap, schemaElementMap, colVars); err != nil {
			instr.cancelDrainer(fmt.Errorf("cannot read values from parquet [%s] row %d: %s", filePath, bs.RowsRead, err.Error()))
			return bs, instr.waitForDrainer(logger, pCtx)
		}

		// TableCreator: evaluate table column expressions
		tableRecord, err = node.TableCreator.CalculateTableRecordFromSrcVars(false, colVars)
		if err != nil {
			instr.cancelDrainer(fmt.Errorf("cannot populate table record from parquet [%s] row %d: [%s]", filePath, bs.RowsRead, err.Error()))
			return bs, instr.waitForDrainer(logger, pCtx)
		}

		// Check table creator having
		inResult, err = node.TableCreator.CheckTableRecordHavingCondition(tableRecord)
		if err != nil {
			instr.cancelDrainer(fmt.Errorf("cannot check having condition [%s] from parquet [%s] row %d, table record [%v]: [%s]", node.TableCreator.RawHaving, filePath, bs.RowsRead, tableRecord, err.Error()))
			return bs, instr.waitForDrainer(logger, pCtx)
		}

		if inResult {
			err = instr.buildIndexKeys(tableRecord, indexKeyMap)
			if err != nil {
				instr.cancelDrainer(fmt.Errorf("cannot build index keys for table %s from parquet [%s] row %d: [%s]", node.TableCreator.Name, filePath, bs.RowsRead, err.Error()))
				return bs, instr.waitForDrainer(logger, pCtx)
			}

			instr.add(tableRecord, indexKeyMap)
			bs.RowsWritten++
		}
		bs.RowsRead++
	}

	instr.doneSending()
	if err := instr.waitForDrainer(logger, pCtx); err != nil {
		return bs, err
	}

	bs.Elapsed = time.Since(totalStartTime)
	reportWriteTableComplete(logger, pCtx, bs.RowsRead, bs.RowsWritten, bs.Elapsed, len(node.TableCreator.Indexes), instr.NumWorkers)

	return bs, nil
}
