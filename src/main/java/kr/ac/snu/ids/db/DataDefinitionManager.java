package kr.ac.snu.ids.db;

import com.sleepycat.je.*;
import kr.ac.snu.ids.definition.ColumnDefinition;
import kr.ac.snu.ids.definition.DataType;
import kr.ac.snu.ids.definition.ForeignKeyDefinition;
import kr.ac.snu.ids.definition.TableDefinition;
import kr.ac.snu.ids.exceptions.CharLengthError;
import kr.ac.snu.ids.exceptions.NoSuchTableError;
import kr.ac.snu.ids.exceptions.create.*;
import kr.ac.snu.ids.exceptions.drop.DropReferencedTableError;
import kr.ac.snu.ids.exceptions.drop.DropTableError;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DataDefinitionManager {

    private Database db;

    public DataDefinitionManager(Database db) {
        this.db = db;
    }

    private void validateForeignKey(ForeignKeyDefinition foreignKeyDefinition, List<ColumnDefinition> referingColumn) {

        boolean tableMatchFlag = false;

        try (Cursor iter = db.openCursor(null, null)) {

            DatabaseEntry iterKey = new DatabaseEntry();
            DatabaseEntry iterData = new DatabaseEntry();

            iter.getFirst(iterKey, iterData, LockMode.DEFAULT);

            do {
                TableDefinition i = inflateSchema(iterData.getData());
                // 존재하는 table 참조
                if (i.getTableName().equals(foreignKeyDefinition.getReferencedTableName())) {
                    tableMatchFlag = true;

                    List<String> columnName = i.getColumnList().stream().map(ColumnDefinition::getColumnName).collect(Collectors.toList());

                    // 존재하는 column 참조
                    if (!columnName.containsAll(foreignKeyDefinition.getReferencedColumnList()))
                        throw new ReferenceColumnExistenceError();

                    // Primary 를 참조
                    if (!(foreignKeyDefinition.getReferencedColumnList().size() == i.getPrimaryKeys().size()
                            && foreignKeyDefinition.getReferencedColumnList().containsAll(i.getPrimaryKeys())))
                        throw new ReferenceNonPrimaryKeyDefError();

                    // Column 들이 서로 같은 타입을 가지는 지 확인
                    List<ColumnDefinition> referedColumns = extractColumn(i.getColumnList(), foreignKeyDefinition.getReferencedColumnList());

                    if (referedColumns.size() != referingColumn.size())
                        throw new ReferenceTypeError();

                    for (int n = 0; n < referedColumns.size();n++) {
                        if (!referedColumns.get(n).getDataType().comp(referingColumn.get(n).getDataType()))
                            throw new ReferenceTypeError();
                    }
                    break;
                }
            } while (iter.getNext(iterKey, iterData, LockMode.DEFAULT) == OperationStatus.SUCCESS);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        if (!tableMatchFlag)
            throw new ReferenceTableExistenceError();
    }

    private List<ColumnDefinition> extractColumn(List<ColumnDefinition> origin, List<String> target) {
        return target.stream().map(
                referingKey -> {
                    for (ColumnDefinition item : origin) {
                        if (item.getColumnName().equals(referingKey))
                            return item;
                    }
                    return null;
                }
        ).collect(Collectors.toList());
    }

    private boolean validateSchema(TableDefinition tableDefinition) throws CreateTableError {
        try (Cursor cursor = db.openCursor(null, null)) {
            DatabaseEntry targetKey = new DatabaseEntry(tableDefinition.getTableName().getBytes(StandardCharsets.UTF_8));
            DatabaseEntry dataBody = new DatabaseEntry();

            // Char 타입의 길이가 1보다 작음
            if (tableDefinition.getColumnList().stream().anyMatch(item -> item.getDataType().getDataType() == DataType.CHARACTER && item.getDataType().getCharLength() < 1))
                throw new CharLengthError();

            // 같은 이름의 테이블 존재
            if (cursor.get(targetKey, dataBody, Get.SEARCH, null) != null)
                throw new TableExistenceError();

            // column 이름 중복
            List<String> columnNames = tableDefinition.getColumnList().stream().map(ColumnDefinition::getColumnName).collect(Collectors.toList());
            Set<String> uniqueNames = new HashSet<>(columnNames);
            if (uniqueNames.size() < columnNames.size())
                throw new DuplicateColumnDefError();

            // primary key 여러번 선언
            if (tableDefinition.getPrimaryKeys().size() == 1 && tableDefinition.getPrimaryKeys().get(0).equals("1dirty"))
                throw new DuplicatePrimaryKeyDefError();

            // 존재하지 않는 칼럼을 primary key로 정의.
            tableDefinition.getPrimaryKeys().forEach(pk -> {
                if (!columnNames.contains(pk))
                    throw new NonExistingColumnDefError(pk);
            });

            // 존재하지 않는 칼럼을 foreign key로 정의.
            for (ForeignKeyDefinition foreignKeyDefinition : tableDefinition.getForeignKeys()) {
                foreignKeyDefinition.getReferencingColumnList().forEach(fk -> {
                    if (!columnNames.contains(fk))
                        throw new NonExistingColumnDefError(fk);
                });
            }

            // ForeignKey 가 기존에 존재하지 않는 table과 column을 참조.
            for (ForeignKeyDefinition foreignKeyDefinition : tableDefinition.getForeignKeys()) {
                validateForeignKey(foreignKeyDefinition, extractColumn(tableDefinition.getColumnList(), foreignKeyDefinition.getReferencingColumnList()));


            }
//            tableDefinition.getForeignKeys().forEach(this::validateForeignKey);

            // primary key 인 column 들에 not null 부여
            for (ColumnDefinition item : tableDefinition.getColumnList()) {
                if (tableDefinition.getPrimaryKeys().contains(item.getColumnName())) {
                    item.setConstraint("not null");
                }
            }
        }


        return true;
    }

    public void createSchema(TableDefinition tableDefinition) {
        byte[] serializeSchema;

        validateSchema(tableDefinition);
        try (Cursor cursor = db.openCursor(null, null)) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(tableDefinition);
            serializeSchema = baos.toByteArray();

            DatabaseEntry key = new DatabaseEntry(tableDefinition.getTableName().getBytes(StandardCharsets.UTF_8));
            DatabaseEntry value = new DatabaseEntry(serializeSchema);

            cursor.put(key, value);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void dropSchema(String tableName) throws NoSuchTableError, DropTableError {

        try (Cursor cursor = db.openCursor(null, null)) {
            DatabaseEntry targetKey = new DatabaseEntry(tableName.getBytes(StandardCharsets.UTF_8));
            DatabaseEntry dataBody = new DatabaseEntry();
            if (cursor.get(targetKey, dataBody, Get.SEARCH, null) == null)
                throw new NoSuchTableError();


            DatabaseEntry iterKey = new DatabaseEntry();
            DatabaseEntry iterData = new DatabaseEntry();
            Cursor iter = db.openCursor(null, null);
            iter.getFirst(iterKey, iterData, LockMode.DEFAULT);

            do {
                TableDefinition i = inflateSchema(iterData.getData());
                if (i.isReferingTable(tableName)) {
                    throw new DropReferencedTableError(tableName);
                }
            } while (iter.getNext(iterKey, iterData, LockMode.DEFAULT) == OperationStatus.SUCCESS);

            iter.close();

            cursor.delete();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public TableDefinition getSchema(String tableName) throws NoSuchTableError {
        TableDefinition tableDefinition = null;

        try (Cursor cursor = db.openCursor(null, null)) {
            DatabaseEntry searchKey = new DatabaseEntry(tableName.getBytes(StandardCharsets.UTF_8));
            DatabaseEntry dataBody = new DatabaseEntry();

            if (cursor.get(searchKey, dataBody, Get.SEARCH, null) == null)
                throw new NoSuchTableError();

            tableDefinition = inflateSchema(dataBody.getData());
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return tableDefinition;
    }

    public List<String> getTableNames() {
        List<String> tables = new ArrayList<>();

        try (Cursor iter = db.openCursor(null, null)) {
            DatabaseEntry iterKey = new DatabaseEntry();
            DatabaseEntry iterData = new DatabaseEntry();

            if (iter.getFirst(iterKey, iterData, LockMode.DEFAULT) != OperationStatus.NOTFOUND) {
                do {
                    String keyName = new String(iterKey.getData(), StandardCharsets.UTF_8);
                    tables.add(keyName);
                } while (iter.getNext(iterKey, iterData, LockMode.DEFAULT) == OperationStatus.SUCCESS);
            }
        }
        return tables;
    }

    private TableDefinition inflateSchema(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        ObjectInputStream ois = new ObjectInputStream(bais);
        return (TableDefinition) ois.readObject();
    }

    // 바이트 배열로 생성된 직렬화 데이터를 base64로 변환
//    System.out.println(Base64.getEncoder().encodeToString(serializedMember));

}
