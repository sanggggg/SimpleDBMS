package kr.ac.snu.ids.definition;

import java.io.Serializable;

public class ColumnDefinition implements Serializable {

    String columnName;
    DataTypeDefinition dataType;
    String constraint;

    public String getColumnName() {
        return columnName;
    }

    public DataTypeDefinition getDataType() {
        return dataType;
    }

    public String getConstraint() {
        return constraint;
    }

    public void setConstraint(String constraint) {
        this.constraint = constraint;
    }

    ColumnDefinition(String columnName, DataTypeDefinition dataType, String constraint) {
        this.columnName = columnName;
        this.dataType = dataType;
        this.constraint = constraint;
    }

    public static class Builder {
        private String columnName;
        private DataTypeDefinition dataType;
        private String constraint = "";

        public Builder setColumnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        public Builder setDataType(DataTypeDefinition dataType) {
            this.dataType = dataType;
            return this;
        }

        public Builder setConstraint(String constraint) {
            this.constraint = constraint;
            return this;
        }

        public ColumnDefinition create() {
            return new ColumnDefinition(columnName, dataType, constraint);
        }
    }

    @Override
    public String toString() {
        return String.format("%-20s%-15s%-15s", columnName, dataType.toString(), constraint.equals("not null") ? "N" : "Y");
    }
}
