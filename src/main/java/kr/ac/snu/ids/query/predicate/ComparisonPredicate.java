package kr.ac.snu.ids.query.predicate;

import kr.ac.snu.ids.db.ComparableValue;
import kr.ac.snu.ids.db.TupleData;
import kr.ac.snu.ids.definition.DataType;
import kr.ac.snu.ids.exceptions.where.WhereAmbiguousReferenceError;
import kr.ac.snu.ids.exceptions.where.WhereColumnNotExistError;
import kr.ac.snu.ids.exceptions.where.WhereIncomparableError;
import kr.ac.snu.ids.exceptions.where.WhereTableNotSpecifiedError;

public class ComparisonPredicate implements BooleanCondition {

    private ComparableValue left;
    private ComparableValue right;
    private String operator;

    public void setLeft(ComparableValue left) {
        this.left = left;
    }

    public void setRight(ComparableValue right) {
        this.right = right;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    private ComparableValue fillData(ComparableValue val, TupleData tuple) {
        String tableName = null;
        String colName;
        if (val.getValue().contains(".")) {
            tableName = val.getValue().split(".")[0];
            colName = val.getValue().split(".")[1];
        } else {
            colName = val.getValue();
        }

        String identifier;

        if (tableName != null) {
            identifier = tableName.concat(".").concat(colName);
        } else {
            identifier = colName;
        }

        if (tableName != null && tuple.getTableName().contains(tableName)) throw new WhereTableNotSpecifiedError();

        if (tuple.getAmbiguous().contains(identifier)) throw new WhereAmbiguousReferenceError();

        if (!tuple.getEntry().containsKey(identifier)) throw new WhereColumnNotExistError();

        return new ComparableValue(tuple.getEntryType().get(identifier), tuple.getEntry().get(identifier));
    }

    @Override
    public boolean execute(TupleData tuple) {
        if (left.getDataType() == DataType.TABLECOL) left = fillData(left, tuple);
        if (right.getDataType() == DataType.TABLECOL) right = fillData(right, tuple);

        if (left.getDataType() != right.getDataType()) throw new WhereIncomparableError();

        int compResult;

        switch (left.getDataType()) {
            case INTEGER:
                compResult = Integer.compare(Integer.parseInt(left.getValue()), Integer.parseInt(right.getValue()));
                break;
            case CHARACTER:
            case DATE:
                compResult = left.getValue().compareTo(right.getValue());
                break;
            default:
                return false;
        }

        switch (compResult) {
            case -1:
                return operator.equals("<") || operator.equals("<=") || operator.equals("!=");
            case 0:
                return operator.equals("<=") || operator.equals(">=") || operator.equals("=");
            case 1:
                return operator.equals(">") || operator.equals(">=") || operator.equals("!=");
        }
        return false;
    }

    @Override
    public String toString() {
        return "ComparisonPredicate{" +
                "left=" + left +
                ", right=" + right +
                ", operator='" + operator + '\'' +
                '}';
    }
}
