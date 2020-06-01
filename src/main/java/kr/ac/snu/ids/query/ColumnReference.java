package kr.ac.snu.ids.query;

import java.util.Objects;

public class ColumnReference {
    private String columnName = null;
    private String tableName = null;
    private String alias = null;

    public String getColumnName() {
        return columnName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getAlias() {
        return alias;
    }

    public String getIdentifier() {
        if (tableName != null) return tableName.concat(".").concat(columnName);
        return columnName;
    }

    public String getShowName() {
        if (alias != null) return alias;
        if (tableName != null) return tableName.concat(".").concat(columnName);
        return columnName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnReference)) return false;
        ColumnReference that = (ColumnReference) o;
        return columnName.equals(that.columnName) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(alias, that.alias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, tableName, alias);
    }

    public ColumnReference(String columnName, String tableName, String alias) {
        this.columnName = columnName;
        this.tableName = tableName;
        this.alias = alias;
    }
}
