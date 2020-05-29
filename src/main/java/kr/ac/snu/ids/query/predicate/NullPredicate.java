package kr.ac.snu.ids.query.predicate;

import kr.ac.snu.ids.db.TupleData;
import kr.ac.snu.ids.exceptions.where.WhereAmbiguousReferenceError;
import kr.ac.snu.ids.exceptions.where.WhereColumnNotExistError;
import kr.ac.snu.ids.exceptions.where.WhereTableNotSpecifiedError;

public class NullPredicate implements BooleanCondition {
    private Boolean isNull;
    private String tableName;
    private String colName;

    public void setNull(Boolean aNull) {
        isNull = aNull;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    @Override
    public boolean execute(TupleData tuple) {
        String identifier;

        if (tableName != null) {
            identifier = tableName.concat(".").concat(colName);
        } else {
            identifier = colName;
        }

        if (tableName != null && tuple.getTableName().contains(tableName)) throw new WhereTableNotSpecifiedError();

        if (tuple.getAmbiguous().contains(identifier)) throw new WhereAmbiguousReferenceError();

        if (!tuple.getEntry().containsKey(identifier)) throw new WhereColumnNotExistError();

        return isNull == (tuple.getEntry().get(identifier) == null);
    }

    @Override
    public String toString() {
        return "NullPredicate{" +
                "isNull=" + isNull +
                ", tableName='" + tableName + '\'' +
                ", colName='" + colName + '\'' +
                '}';
    }
}
