package kr.ac.snu.ids.query;

import kr.ac.snu.ids.db.ComparableValue;

import java.util.List;

public class InsertQuery {
    private String tableName;
    private List<String> columnName;
    private List<ComparableValue> valueList;

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumnName() {
        return columnName;
    }

    public void setValueList(List<ComparableValue> valueList) {
        this.valueList = valueList;
    }

    public List<ComparableValue> getValueList() {
        return valueList;
    }

    public InsertQuery(String tableName, List<String> columnName, List<ComparableValue> valueList) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.valueList = valueList;
    }

    public static class Builder {
        private String tableName;
        private List<String> columnName;
        private List<ComparableValue> valueList;

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder setColumnName(List<String> columnName) {
            this.columnName = columnName;
            return this;
        }

        public void setValueList(List<ComparableValue> valueList) {
            this.valueList = valueList;
        }

        public InsertQuery create() {
            return new InsertQuery(tableName, columnName, valueList);
        }
    }

    @Override
    public String toString() {
        return "InsertQuery{" +
                "tableName='" + tableName + '\'' +
                ", columnName=" + columnName +
                ", valueList=" + valueList +
                '}';
    }
}
