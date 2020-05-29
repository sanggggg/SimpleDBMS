package kr.ac.snu.ids.query;

import kr.ac.snu.ids.query.predicate.BooleanCondition;

import java.util.List;

public class SelectQuery {
    private List<String> selectColumnList;
    private List<String> tableReferenceList;
    private BooleanCondition condition;

    public SelectQuery(List<String> selectColumnList, List<String> tableReferenceList, BooleanCondition condition) {
        this.selectColumnList = selectColumnList;
        this.tableReferenceList = tableReferenceList;
        this.condition = condition;
    }

    @Override
    public String toString() {
        return "SelectQuery{" +
                "selectColumnList=" + selectColumnList +
                ", tableReferenceList=" + tableReferenceList +
                ", condition=" + condition +
                '}';
    }

    public static class Builder {
        private List<String> selectColumnList;
        private List<String> tableReferenceList;
        private BooleanCondition condition;

        public Builder setSelectColumnList(List<String> selectColumnList) {
            this.selectColumnList = selectColumnList;
            return this;
        }

        public Builder setTableReferenceList(List<String> tableReferenceList) {
            this.tableReferenceList = tableReferenceList;
            return this;
        }

        public Builder setCondition(BooleanCondition condition) {
            this.condition = condition;
            return this;
        }

        public SelectQuery create() {
            return new SelectQuery(selectColumnList, tableReferenceList, condition);
        }
    }

}
