package kr.ac.snu.ids.db;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RowIterator {
    private Cursor cursor;
    private boolean toFirst;

    public RowIterator(String tableName) {
        cursor = BerKeleyDB.getTableInstance(tableName).openCursor(null, null);
        openedCursors.add(cursor);
        toFirst = true;
    }

    void setFirst() {
        toFirst = true;
    }

    public byte[] serializeRow(Map<String, String> row) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = null;
        byte[] serializeEntry = null;

        try {
            oos = new ObjectOutputStream(baos);
            oos.writeObject(row);
            serializeEntry = baos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return serializeEntry;
    }

    public boolean insert(Map<String, String> row) {
        byte[] serializedRow = serializeRow(row);
        if (serializedRow == null) return false;

        String pk = String.valueOf(row.hashCode());
        DatabaseEntry key = new DatabaseEntry(pk.getBytes(StandardCharsets.UTF_8));
        DatabaseEntry value = new DatabaseEntry(serializedRow);
        return cursor.put(key, value) == OperationStatus.SUCCESS;
    }

    public boolean put(Map<String, String> row) {
        if (!toFirst) {
            byte[] serializedRow = serializeRow(row);
            if (serializedRow == null) return false;

            DatabaseEntry value = new DatabaseEntry(serializedRow);
            return cursor.putCurrent(value) == OperationStatus.SUCCESS;
        } else {
            return false;
        }

    }

    HashMap<String, String> next() {
        DatabaseEntry foundKey = new DatabaseEntry();
        DatabaseEntry foundValue = new DatabaseEntry();

        if (toFirst) {
            if (cursor.getFirst(foundKey, foundValue, LockMode.DEFAULT) == OperationStatus.NOTFOUND)
                return null;
            toFirst = false;
        } else {
            if (cursor.getNext(foundKey, foundValue, LockMode.DEFAULT) == OperationStatus.NOTFOUND)
                return null;
        }

        ByteArrayInputStream bais = new ByteArrayInputStream(foundValue.getData());
        ObjectInputStream ois;
        try {
            ois = new ObjectInputStream(bais);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

        try {
            return (HashMap<String, String>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    boolean delete() {
        if (!toFirst) {
            return cursor.delete() == OperationStatus.SUCCESS;
        } else {
            return false;
        }
    }

    void close() {
        if (cursor != null) {
            openedCursors.remove(cursor);
            cursor.close();
            cursor = null;
        }
    }

    public static List<Cursor> openedCursors = new ArrayList<>();

}
