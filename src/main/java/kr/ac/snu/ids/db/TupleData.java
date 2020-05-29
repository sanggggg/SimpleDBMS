package kr.ac.snu.ids.db;

import kr.ac.snu.ids.definition.DataType;

import java.util.HashMap;
import java.util.List;

public class TupleData {
    private HashMap<String, String> entry;
    private List<String> ambiguous;
    private List<String> tableName;
    private HashMap<String, DataType> entryType;

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
}
