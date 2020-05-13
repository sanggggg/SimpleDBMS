package kr.ac.snu.ids.definition;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public class TableDefinition implements Serializable {

    private String tableName;
    private ArrayList<ColumnDefinition> columnList;
    private ArrayList<String> primaryKeys;
    private ArrayList<ForeignKeyDefinition> foreignKeys;

    public String getTableName() {
        return tableName;
    }

    public ArrayList<ColumnDefinition> getColumnList() {
        return columnList;
    }

    public ArrayList<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public ArrayList<ForeignKeyDefinition> getForeignKeys() {
        return foreignKeys;
    }

    TableDefinition(String tableName, ArrayList<ColumnDefinition> columnList, ArrayList<String> primaryKeys, ArrayList<ForeignKeyDefinition> foreignKeys) {
        this.tableName = tableName;
        this.columnList = columnList;
        this.primaryKeys = primaryKeys;
        this.foreignKeys = foreignKeys;
    }

    public static class Builder {
        private String tableName;
        private ArrayList<ColumnDefinition> columnList = new ArrayList<ColumnDefinition>();
        private ArrayList<String> primaryKeys = new ArrayList<String>();
        private ArrayList<ForeignKeyDefinition> foreignKeys = new ArrayList<ForeignKeyDefinition>();
        private boolean dirty = false;

        public Builder setTableName(String tableName) {
            this.tableName = tableName.toLowerCase();
            return this;
        }

        public Builder addColumn(ColumnDefinition column) {
            this.columnList.add(column);
            return this;
        }

        public Builder addForeignKey(ForeignKeyDefinition foreignKey) {
            this.foreignKeys.add(foreignKey);
            return this;
        }

        public Builder setPrimaryKeys(ArrayList<String> primaryKeys) {
            if (!this.primaryKeys.isEmpty()) {
                dirty = true;
            } else {
                ListIterator<String> iter = primaryKeys.listIterator();
                while (iter.hasNext()) { iter.set(iter.next().toLowerCase()); }

                this.primaryKeys = primaryKeys;
            }
            return this;
        }

        public TableDefinition create() {
            ArrayList<String> dirtyArray = new ArrayList<>();
            dirtyArray.add("1dirty");
            if (dirty) primaryKeys = dirtyArray;
            return new TableDefinition(tableName, columnList, primaryKeys, foreignKeys);
        }

    }

    public boolean isReferingTable(String tableName) {
        return foreignKeys.stream().anyMatch(item -> item.getReferencedTableName().equals(tableName));
    }

    @Override
    public String toString() {
        List<String> columnInfos = new ArrayList<String>();

        for (ColumnDefinition col : columnList) {
            List<String> keyInfo = new ArrayList<String>();

            if (primaryKeys.contains(col.columnName)) {
                keyInfo.add("PRI");
            }
            if (foreignKeys.stream().anyMatch(item -> item.hasRefColumn(col.columnName))) {
                keyInfo.add("FOR");
            }

            columnInfos.add(col.toString() + String.join("/", keyInfo));
        }


        return "-------------------------------------------------\n" +
                String.format("tableName [%s]\n", tableName) +
                String.format("%-20s%-15s%-15s%-15s\n", "column_name", "type", "null", "key") +
                String.join("\n", columnInfos) + "\n" +
                "-------------------------------------------------";
    }
}
