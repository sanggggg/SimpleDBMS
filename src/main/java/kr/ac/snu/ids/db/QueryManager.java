package kr.ac.snu.ids.db;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import kr.ac.snu.ids.definition.ColumnDefinition;
import kr.ac.snu.ids.definition.DataType;
import kr.ac.snu.ids.definition.TableDefinition;
import kr.ac.snu.ids.exceptions.NoSuchTableError;
import kr.ac.snu.ids.exceptions.insert.InsertColumnNonNullableError;
import kr.ac.snu.ids.exceptions.insert.InsertTypeMismatchError;
import kr.ac.snu.ids.query.InsertQuery;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class QueryManager {

    private static Database schemaDb = BerKeleyDB.getInstance();

    public static void insertQuery(InsertQuery query) throws NoSuchTableError {
        TableDefinition table = DataDefinitionManager.getSchema(query.getTableName());
        try (Cursor cursor = schemaDb.openCursor(null, null)) {
            HashMap<String, String> entry = new HashMap<>();
            String pk = null;

            // 타입이 valid 한지 확인 : InsertTypeMismatchError
            // null 위반 확인 : InsertColumnNonNullableError
            // column name 존재 확인 : InsertColumnExistenceError
            if (query.getValueList().size() != table.getColumnList().size()) throw new InsertTypeMismatchError();
            if (query.getColumnName() == null || query.getColumnName().size() == 0) {
                for (int iter = 0; iter < query.getValueList().size(); iter++) {
                    ColumnDefinition col = table.getColumnList().get(iter);
                    String value = query.getValueList().get(iter);
                    DataType valType;
                    switch (value.split("'")[0]) {
                        case "int":
                            valType = DataType.INTEGER;
                            break;
                        case "char":
                            valType = DataType.CHARACTER;
                            break;
                        case "date":
                            valType = DataType.DATE;
                            break;
                        case "null":
                            valType = null;
                            break;
                        default:
                            throw new InsertTypeMismatchError();
                    }

                    if (valType == null) {
                        if (col.getConstraint().equals("not null"))
                            throw new InsertColumnNonNullableError(col.getColumnName());
                        entry.put(col.getColumnName(), null);
                    } else {
                        if (col.getDataType().getDataType() != valType) throw new InsertTypeMismatchError();
                        String valData = value.split("'")[1];
                        // Char length 맞춰 cutting
                        if (col.getDataType().getDataType() == DataType.CHARACTER && col.getDataType().getCharLength() < valData.length()) {
                            valData = valData.substring(0, col.getDataType().getCharLength());
                        }
                        entry.put(col.getColumnName(), valData);
                    }
                }
            } else {
                // TODO: column name 명시시에 어떻게?
            }

            // primary key 제약 확인
            // TODO: primary key 제약 확인, find 관련 query 완성 후

            // foreign key 제약 확인
            // TODO: foreign key 제약 확인, find 관련 query 완성 후
            System.out.println(entry.toString());

            // Insert to BerkeleyDB
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(entry);
            byte[] serializeEntry = baos.toByteArray();

            pk = "testing";
            DatabaseEntry key = new DatabaseEntry(pk.getBytes(StandardCharsets.UTF_8));
            DatabaseEntry value = new DatabaseEntry(serializeEntry);

            cursor.put(key, value);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
