package kr.ac.snu.ids.query;

import kr.ac.snu.ids.query.predicate.BooleanCondition;

import java.util.List;

public class SelectQuery {
    private List<TableReference> tableReferenceList;
    private List<ColumnReference> columnReferenceList;
    private BooleanCondition condition;

    public SelectQuery(List<ColumnReference> columnReferenceList, List<TableReference> tableReferenceList, BooleanCondition condition) {
        this.columnReferenceList = columnReferenceList;
        this.tableReferenceList = tableReferenceList;
        this.condition = condition;
    }

    public List<ColumnReference> getColumnReferenceList() {
        return columnReferenceList;
    }

    public List<TableReference> getTableReferenceList() {
        return tableReferenceList;
    }

    public BooleanCondition getCondition() {
        return condition;
    }

    public static class Builder {
        private List<ColumnReference> columnReferenceList;
        private List<TableReference> tableReferenceList;
        private BooleanCondition condition;

        public Builder setColumnReferenceList(List<ColumnReference> columnReferenceList) {
            this.columnReferenceList = columnReferenceList;
            return this;
        }

        public Builder setTableReferenceList(List<TableReference> tableReferenceList) {
            this.tableReferenceList = tableReferenceList;
            return this;
        }

        public Builder setCondition(BooleanCondition condition) {
            this.condition = condition;
            return this;
        }

        public SelectQuery create() {
            return new SelectQuery(columnReferenceList, tableReferenceList, condition);
        }
    }

}
