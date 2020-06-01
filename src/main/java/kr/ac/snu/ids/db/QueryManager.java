package kr.ac.snu.ids.db;

import com.sleepycat.je.Database;
import kr.ac.snu.ids.definition.ColumnDefinition;
import kr.ac.snu.ids.definition.DataType;
import kr.ac.snu.ids.definition.ForeignKeyDefinition;
import kr.ac.snu.ids.definition.TableDefinition;
import kr.ac.snu.ids.exceptions.NoSuchTableError;
import kr.ac.snu.ids.exceptions.insert.*;
import kr.ac.snu.ids.exceptions.select.SelectColumnResolveError;
import kr.ac.snu.ids.exceptions.select.SelectTableExistenceError;
import kr.ac.snu.ids.query.*;
import kr.ac.snu.ids.query.predicate.WhereBoolean;

import java.util.*;
import java.util.stream.Collectors;

public class QueryManager {

    private static Database schemaDb = BerKeleyDB.getInstance();

    public static void insertQuery(InsertQuery query) throws NoSuchTableError {
        TableDefinition table = DataDefinitionManager.getSchema(query.getTableName());

        Map<String, String> entry = new HashMap<>();
        String pk = null;

        if (query.getValueList().size() != table.getColumnList().size()) throw new InsertTypeMismatchError();

        // column name 이 존재할 경우 맞춰 순서를 다시 변경
        // column name 존재 확인 : InsertColumnExistenceError
        // 타입이 valid 한 지 확인 : InsertTypeMismatchError
        if (query.getColumnName() != null && query.getColumnName().size() != 0) {
            if (query.getColumnName().size() != table.getColumnList().size()) throw new InsertTypeMismatchError();

            List<String> validColumnNameList = table.getColumnList().stream().map(ColumnDefinition::getColumnName).collect(Collectors.toList());
            ComparableValue[] reorderedValues = new ComparableValue[query.getColumnName().size()];

            for (int iter = 0; iter < query.getColumnName().size(); iter++) {
                String columnName = query.getColumnName().get(iter);
                ComparableValue value = query.getValueList().get(iter);

                int index;

                if ((index = validColumnNameList.indexOf(columnName)) < 0)
                    throw new InsertColumnExistenceError(columnName);

                reorderedValues[index] = value;
            }

            query.setValueList(Arrays.asList(reorderedValues));
        }

        // 타입이 valid 한지 확인 : InsertTypeMismatchError
        // null 위반 확인 : InsertColumnNonNullableError
        for (int iter = 0; iter < query.getValueList().size(); iter++) {
            ColumnDefinition col = table.getColumnList().get(iter);
            ComparableValue value = query.getValueList().get(iter);

            if (value.getValue() == null) {
                if (Objects.equals(col.getConstraint(), "not null"))
                    throw new InsertColumnNonNullableError(col.getColumnName());
                entry.put(col.getColumnName(), null);
            } else {
                if (col.getDataType().getDataType() != value.getDataType()) throw new InsertTypeMismatchError();
                // Char length 맞춰 cutting
                String valData = value.getValue();
                if (col.getDataType().getDataType() == DataType.CHARACTER && col.getDataType().getCharLength() < value.getValue().length()) {
                    valData = valData.substring(0, col.getDataType().getCharLength());
                }
                entry.put(col.getColumnName(), valData);
            }
        }

        // primary key 제약 확인
        List<String> tuplePkey = new ArrayList<>();
        for (String primaryKey : table.getPrimaryKeys()) {
            tuplePkey.add(entry.get(primaryKey));
        }
        if (existCheck(tuplePkey, table.getPrimaryKeys(), table.getTableName()))
            throw new InsertDuplicatePrimaryKeyError();

        // foreign key 제약 확인
        for (ForeignKeyDefinition foreignKeyDefinition : table.getForeignKeys()) {
            List<String> tupleFkey = new ArrayList<>();
            for (String foreignKey : foreignKeyDefinition.getReferencingColumnList()) {
                tupleFkey.add(entry.get(foreignKey));
            }
            if (!existCheck(tupleFkey, foreignKeyDefinition.getReferencedColumnList(), foreignKeyDefinition.getReferencedTableName()))
                throw new InsertReferentialIntegrityError();
        }

        // Insert to BerkeleyDB
        RowIterator iterator = new RowIterator(query.getTableName());
        iterator.insert(entry);
        iterator.close();
    }

    public static int deleteQuery(DeleteQuery query) {
        // Table 존재 여부 확인
        TableDefinition tableDefinition = DataDefinitionManager.getSchema(query.getTableName());
        int deleteCount = 0;
        int failedCount = 0;
        RowIterator iter = new RowIterator(tableDefinition.getTableName());
        Map<String, String> row;

        // Consistency check 가 필요한 table 정보
        while ((row = iter.next()) != null) {
            TupleData tuple = new TupleData();

            tuple = tuple.concatRow(tableDefinition, row);

            if (query.getCondition() == null || query.getCondition().execute(tuple) == WhereBoolean.TRUE) {
                TableIterator tableIterator = new TableIterator();
                TableDefinition iTableDefinition;
                List<TableDefinition> referringTables = new ArrayList<>();
                boolean deletable = true;

                while ((iTableDefinition = tableIterator.next()) != null) {
                    if (iTableDefinition.isReferingTable(tableDefinition.getTableName())) {
                        for (ForeignKeyDefinition referringFkey : iTableDefinition.getForeignKeys()) {
                            if (referringFkey.getReferencedTableName().equals(tableDefinition.getTableName())) {
                                List<String> candidateFkey = new ArrayList<>();
                                for (String columnName : referringFkey.getReferencedColumnList()) {
                                    candidateFkey.add(row.get(columnName));
                                }

                                if (!maintainConsistency(iTableDefinition, referringFkey.getReferencingColumnList(), candidateFkey, true)) {
                                    deletable = false;
                                    break;
                                } else {
                                    if (!referringTables.contains(iTableDefinition))
                                        referringTables.add(iTableDefinition);
                                }
                            }
                        }
                    }
                }
                tableIterator.close();

                if (deletable) {
                    for (TableDefinition referringTable : referringTables) {
                        for (ForeignKeyDefinition referringFkey : referringTable.getForeignKeys()) {
                            if (referringFkey.getReferencedTableName().equals(tableDefinition.getTableName())) {
                                List<String> candidateFkey = new ArrayList<>();
                                for (String columnName : referringFkey.getReferencedColumnList()) {
                                    candidateFkey.add(row.get(columnName));
                                }
                                maintainConsistency(referringTable, referringFkey.getReferencingColumnList(), candidateFkey, false);
                            }
                        }
                    }
                    iter.delete();
                    deleteCount++;
                } else {
                    failedCount++;
                }
            }
        }
        iter.close();

        if (failedCount != 0)
            System.out.println(String.format("[%d] row(s) are not deleted due to referential integrity", failedCount));
        return deleteCount;
    }

    private static boolean maintainConsistency(TableDefinition tableDefinition, List<String> keyColumns, List<String> targetData, boolean onlyChecking) {
        RowIterator iter = new RowIterator(tableDefinition.getTableName());
        Map<String, String> row;
        boolean cannotDelete = false;

        if (tableDefinition.getColumnList().stream().anyMatch(it -> keyColumns.contains(it.getColumnName()) && Objects.equals(it.getConstraint(), "not null"))) {
            cannotDelete = true;
        }

        List<Map<String, String>> adjustedRows = new ArrayList<>();

        while ((row = iter.next()) != null) {
            boolean match = true;
            for (String keyColumn : keyColumns) {
                if (!Objects.equals(row.get(keyColumn), targetData.get(keyColumns.indexOf(keyColumn)))) {
                    match = false;
                    break;
                }
            }

            if (match) {
                if (cannotDelete) {
                    iter.close();
                    return false;
                } else if (!onlyChecking) {
                    for (String keyColumn : keyColumns) {
                        row.put(keyColumn, null);
                    }
                    adjustedRows.add(row);
                    iter.delete();
                }
            }
        }
        adjustedRows.forEach(iter::insert);
        iter.close();
        return true;
    }

    private static boolean existCheck(List<String> tuplePkey, List<String> primaryKeys, String tableName) {
        // No pkey
        if (primaryKeys.size() == 0) return false;

        RowIterator iter = new RowIterator(tableName);
        Map<String, String> row;

        while ((row = iter.next()) != null) {
            boolean match = true;
            for (int i = 0; i < tuplePkey.size(); i++) {
                if (!Objects.equals(row.get(primaryKeys.get(i)), tuplePkey.get(i))) {
                    match = false;
                    break;
                }
            }
            if (match) {
                iter.close();
                return true;
            }
        }
        iter.close();
        return false;
    }

    private static List<TupleData> cartesianProduct(List<TupleData> old, TableDefinition tableDefinition, TableReference tableReference) {
        List<TupleData> attached = new ArrayList<>();

        // Alias 가 존재할 시 alias 로 만 검색가능
        for (TupleData oldTuple : old) {
            RowIterator iter = new RowIterator(tableDefinition.getTableName());
            Map<String, String> row;
            while ((row = iter.next()) != null) {
                attached.add(oldTuple.concatRow(tableDefinition, tableReference, row));
            }
            iter.close();
        }
        return attached;
    }

    public static void selectQuery(SelectQuery query) throws NoSuchTableError {
        List<TableDefinition> tableList = new ArrayList<>();
        List<TupleData> accumTuple = new ArrayList<>();
        List<String> validColumnReference = new ArrayList<>();
        accumTuple.add(new TupleData());

        // Table name 이 존재하는지 확인
        // SelectTableExistenceError 체크
        for (TableReference tableReference : query.getTableReferenceList()) {
            try {
                tableList.add(DataDefinitionManager.getSchema(tableReference.getTableName()));
            } catch (NoSuchTableError err) {
                throw new SelectTableExistenceError(tableReference.getTableName());
            }
        }

        // table 들을 cartesian product
        for (int i = 0; i < query.getTableReferenceList().size(); i++) {
            accumTuple = cartesianProduct(accumTuple,
                    tableList.get(i),
                    query.getTableReferenceList().get(i));
        }


        // where 문으로 filtering
        if (query.getCondition() != null) {
            accumTuple = accumTuple
                    .stream()
                    .filter(tup -> query.getCondition().execute(tup) == WhereBoolean.TRUE)
                    .collect(Collectors.toList());
        }

        // '*' 을 실제 table로 풀어 적기.
        while (query.getColumnReferenceList().contains(new ColumnReference("*", null, null))) {
            List<ColumnReference> additionalColumn = new ArrayList<>();
            for (TableDefinition tableDefinition : tableList) {
                for (ColumnDefinition columnDefinition : tableDefinition.getColumnList()) {
                    additionalColumn.add(new ColumnReference(
                            columnDefinition.getColumnName(),
                            query.getTableReferenceList().get(tableList.indexOf(tableDefinition)).getShowName(),
                            columnDefinition.getColumnName()));
                }
            }

            int indexAsterisk = query.getColumnReferenceList().indexOf(new ColumnReference("*", null, null));
            query.getColumnReferenceList().remove(indexAsterisk);
            query.getColumnReferenceList().addAll(indexAsterisk, additionalColumn);
        }

        // columnReference 를 resolve 함
        validColumnReferenceCheck(query);

        // 실제 콘솔에 출력
        for (ColumnReference columnReference : query.getColumnReferenceList()) {
            columnReference.getIdentifier();
        }


        printTuples(accumTuple, query.getColumnReferenceList());
    }

    private static void validColumnReferenceCheck(SelectQuery query) {
        List<String> validIdentifier = new ArrayList<>();
        List<String> ambiguousIdentifier = new ArrayList<>();

        for (TableReference tableReference : query.getTableReferenceList()) {
            for (ColumnDefinition columnDefinition : DataDefinitionManager.getSchema(tableReference.getTableName()).getColumnList()) {
                String fullName, shortName;
                fullName = tableReference.getShowName().concat(".").concat(columnDefinition.getColumnName());
                shortName = columnDefinition.getColumnName();
                if (validIdentifier.contains(fullName))
                    ambiguousIdentifier.add(fullName);
                else
                    validIdentifier.add(fullName);

                if (validIdentifier.contains(shortName))
                    ambiguousIdentifier.add(shortName);
                else
                    validIdentifier.add(shortName);
            }
        }

        for (ColumnReference columnReference : query.getColumnReferenceList()) {
            if (!validIdentifier.contains(columnReference.getIdentifier()) || ambiguousIdentifier.contains(columnReference.getIdentifier())) {
                throw new SelectColumnResolveError(columnReference.getIdentifier());
            }
        }
    }

    private static void printTuples(List<TupleData> tuples, List<ColumnReference> columnReferenceList) {
        String header = "";
        String index = "";
        String row;

        for (ColumnReference columnReference : columnReferenceList) {
            String showName = columnReference.getShowName();
            header = header.concat("+--------------------");
            index = index.concat(String.format("|%20s", showName));
        }

        header = header.concat("+");
        index = index.concat("|");

        System.out.println(header);
        System.out.println(index);
        System.out.println(header);

        for (TupleData tup : tuples) {
            row = "";
            for (String data : tup.getFormattedData(columnReferenceList.stream().map(ColumnReference::getIdentifier).collect(Collectors.toList()))) {
                row = row.concat(String.format("|%20s", data));
            }
            row = row.concat("|");
            System.out.println(row);
        }

        System.out.println(header);
    }

}
