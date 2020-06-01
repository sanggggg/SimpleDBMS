package kr.ac.snu.ids.query;

public class TableReference {
    private String tableName = null;
    private String alias = null;

    public String getTableName() {
        return tableName;
    }

    public String getAlias() {
        return alias;
    }

    public String getShowName() {
        if (alias != null) return alias;
        return tableName;
    }

    public TableReference(String tableName, String alias) {
        this.tableName = tableName;
        this.alias = alias;
    }
}
