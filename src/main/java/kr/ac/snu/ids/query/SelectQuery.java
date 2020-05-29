package kr.ac.snu.ids.query;

import java.util.List;

public class SelectQuery {
    private List<String> selectColumnList;
    private List<String> tableReferenceList;

    public SelectQuery(List<String> selectColumnList, List<String> tableReferenceList) {
        this.selectColumnList = selectColumnList;
        this.tableReferenceList = tableReferenceList;
    }

    @Override
    public String toString() {
        return "SelectQuery{" +
                "selectColumnList=" + selectColumnList +
                ", tableReferenceList=" + tableReferenceList +
                '}';
    }

    public static class Builder {
        private List<String> selectColumnList;
        private List<String> tableReferenceList;

        public Builder setSelectColumnList(List<String> selectColumnList) {
            this.selectColumnList = selectColumnList;
            return this;
        }

        public Builder setTableReferenceList(List<String> tableReferenceList) {
            this.tableReferenceList = tableReferenceList;
            return this;
        }

        public SelectQuery create() {
            return new SelectQuery(selectColumnList, tableReferenceList);
        }
    }

}
