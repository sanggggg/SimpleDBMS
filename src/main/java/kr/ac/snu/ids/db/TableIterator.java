package kr.ac.snu.ids.db;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import kr.ac.snu.ids.definition.TableDefinition;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

public class TableIterator {
    private Cursor cursor;
    private boolean toFirst;

    public TableIterator() {
        cursor = BerKeleyDB.getInstance().openCursor(null, null);
        openedCursors.add(cursor);
        toFirst = true;
    }

    void setFirst() {
        toFirst = true;
    }

    TableDefinition next() {
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
            return (TableDefinition) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
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
