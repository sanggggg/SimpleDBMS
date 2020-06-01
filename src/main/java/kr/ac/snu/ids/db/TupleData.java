package kr.ac.snu.ids.db;

import kr.ac.snu.ids.definition.ColumnDefinition;
import kr.ac.snu.ids.definition.DataType;
import kr.ac.snu.ids.definition.TableDefinition;
import kr.ac.snu.ids.exceptions.select.SelectColumnResolveError;
import kr.ac.snu.ids.query.TableReference;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TupleData {
    private HashMap<String, String> entry = new HashMap<>();
    private List<String> ambiguous = new ArrayList<>();
    private List<String> tableName = new ArrayList<>();
    private HashMap<String, DataType> entryType = new HashMap<>();

    public HashMap<String, DataType> getEntryType() {
        return entryType;
    }

    public void setEntryType(HashMap<String, DataType> entryType) {
        this.entryType = entryType;
    }

    public HashMap<String, String> getEntry() {
        return entry;
    }

    public void setEntry(HashMap<String, String> entry) {
        this.entry = entry;
    }

    public List<String> getAmbiguous() {
        return ambiguous;
    }

    public void setAmbiguous(List<String> ambiguous) {
        this.ambiguous = ambiguous;
    }

    public List<String> getTableName() {
        return tableName;
    }

    public void setTableName(List<String> tableName) {
        this.tableName = tableName;
    }

    public List<String> getFormattedData(List<String> columnNameList) {
        List<String> ret = new ArrayList<>();
        for (String columnName : columnNameList) {
            if (!entry.containsKey(columnName) || ambiguous.contains(columnName))
                throw new SelectColumnResolveError(columnName);
            ret.add(entry.get(columnName));
        }
        return ret;
    }

    public TupleData concatRow(TableDefinition tableDefinition, Map<String, String> row) {
        return this.concatRow(tableDefinition, new TableReference(tableDefinition.getTableName(), null), row);
    }

    public TupleData concatRow(TableDefinition tableDefinition, TableReference tableReference, Map<String, String> row) {
        TupleData tupleData = this.copy();

        tupleData.getTableName().add(tableReference.getShowName());

        for (ColumnDefinition column : tableDefinition.getColumnList()) {
            String fullName = tableReference.getShowName().concat(".").concat(column.getColumnName());

            if (tupleData.getEntry().containsKey(fullName)) {
                tupleData.getAmbiguous().add(fullName);
                continue;
            }

            tupleData.getEntry().put(fullName, row.get(column.getColumnName()));
            tupleData.getEntryType().put(fullName, column.getDataType().getDataType());

            if (tupleData.getEntry().containsKey(column.getColumnName())) {
                tupleData.getAmbiguous().add(column.getColumnName());
                continue;
            }
            tupleData.getEntry().put(column.getColumnName(), row.get(column.getColumnName()));
            tupleData.getEntryType().put(column.getColumnName(), column.getDataType().getDataType());
        }
        return tupleData;
    }

    public List<String> extractColumn(List<String> columnList) {
        return columnList.stream().map(it -> entry.get(it)).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "TupleData{" +
                "entry=" + entry +
                ", ambiguous=" + ambiguous +
                ", tableName=" + tableName +
                ", entryType=" + entryType +
                '}';
    }

    public TupleData copy() {
        HashMap<String, String> entry = new HashMap<>(this.entry);
        List<String> ambiguous = new ArrayList<>(this.ambiguous);
        List<String> tableName = new ArrayList<>(this.tableName);
        HashMap<String, DataType> entryType = new HashMap<>(this.entryType);

        TupleData copied = new TupleData();
        copied.setEntry(entry);
        copied.setEntryType(entryType);
        copied.setAmbiguous(ambiguous);
        copied.setTableName(tableName);

        return copied;
    }
}
