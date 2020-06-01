package kr.ac.snu.ids.definition;

import java.io.Serializable;
import java.util.ArrayList;

public class ForeignKeyDefinition implements Serializable {
    private String referencedTableName;
    private ArrayList<String> referencedColumnList;
    private ArrayList<String> referencingColumnList;

    public String getReferencedTableName() {
        return referencedTableName;
    }

    public ArrayList<String> getReferencedColumnList() {
        return referencedColumnList;
    }

    public ArrayList<String> getReferencingColumnList() {
        return referencingColumnList;
    }

    public ForeignKeyDefinition(String referencedTableName, ArrayList<String> referencedColumnList, ArrayList<String> referencingColumnList) {
        this.referencedTableName = referencedTableName;
        this.referencedColumnList = referencedColumnList;
        this.referencingColumnList = referencingColumnList;
    }

    public boolean hasRefColumn(String columnName) {
        return referencingColumnList.contains(columnName);
    }

    public static class Builder {
        private String referencedTableName;
        private ArrayList<String> referencedColumnList;
        private ArrayList<String> referencingColumnList;

        public Builder setReferencedTableName(String referencedTableName) {
            this.referencedTableName = referencedTableName;
            return this;
        }

        public Builder setReferencedColumn(ArrayList<String> columnList) {
            this.referencedColumnList = columnList;
            return this;
        }

        public Builder setReferencingColumn(ArrayList<String> columnList) {
            this.referencingColumnList = columnList;
            return this;
        }

        public ForeignKeyDefinition create() {
            return new ForeignKeyDefinition(referencedTableName, referencedColumnList, referencingColumnList);
        }
    }

    @Override
    public String toString() {
        return "ForeignKeyDefinition{" +
                "referencedTableName='" + referencedTableName + '\'' +
                ", referencedColumnList=" + referencedColumnList +
                ", referencingColumnList=" + referencingColumnList +
                '}';
    }
}
