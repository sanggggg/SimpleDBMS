package kr.ac.snu.ids.query.predicate;

import kr.ac.snu.ids.db.ComparableValue;
import kr.ac.snu.ids.db.TupleData;
import kr.ac.snu.ids.definition.DataType;
import kr.ac.snu.ids.exceptions.where.WhereAmbiguousReferenceError;
import kr.ac.snu.ids.exceptions.where.WhereColumnNotExistError;
import kr.ac.snu.ids.exceptions.where.WhereIncomparableError;
import kr.ac.snu.ids.exceptions.where.WhereTableNotSpecifiedError;

public class ComparisonPredicate implements BooleanCondition {

    private ComparableValue leftForm;
    private ComparableValue rightForm;
    private String operator;

    public void setLeft(ComparableValue left) {
        this.leftForm = left;
    }

    public void setRight(ComparableValue right) {
        this.rightForm = right;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    private ComparableValue fillData(ComparableValue val, TupleData tuple) {
        String tableName = null;
        String colName;

        if (val.getValue().contains(".")) {
            tableName = val.getValue().split("\\.")[0];
            colName = val.getValue().split("\\.")[1];
        } else {
            colName = val.getValue();
        }

        String identifier;

        if (tableName != null) {
            identifier = tableName.concat(".").concat(colName);
        } else {
            identifier = colName;
        }

        if (tableName != null && !tuple.getTableName().contains(tableName)) throw new WhereTableNotSpecifiedError();

        if (tuple.getAmbiguous().contains(identifier)) throw new WhereAmbiguousReferenceError();

        if (!tuple.getEntry().containsKey(identifier)) throw new WhereColumnNotExistError();

        return new ComparableValue(tuple.getEntryType().get(identifier), tuple.getEntry().get(identifier));
    }

    @Override
    public WhereBoolean execute(TupleData tuple) {
        ComparableValue left;
        ComparableValue right;
        if (leftForm.getDataType() == DataType.TABLECOL) left = fillData(leftForm, tuple);
        else left = leftForm;
        if (rightForm.getDataType() == DataType.TABLECOL) right = fillData(rightForm, tuple);
        else right = rightForm;

        if (left.getDataType() != right.getDataType()) throw new WhereIncomparableError();

        int compResult;

        if (left.getValue() == null || right.getValue() == null) return WhereBoolean.UNDEFINED;

        switch (left.getDataType()) {
            case INTEGER:
                compResult = Integer.compare(Integer.parseInt(left.getValue()), Integer.parseInt(right.getValue()));
                break;
            case CHARACTER:
            case DATE:
                compResult = left.getValue().compareTo(right.getValue());
                break;
            default:
                return WhereBoolean.FALSE;
        }

        if (compResult < 0)
            return operator.equals("<") || operator.equals("<=") || operator.equals("!=") ? WhereBoolean.TRUE : WhereBoolean.FALSE;
        else if (compResult == 0)
            return operator.equals("<=") || operator.equals(">=") || operator.equals("=") ? WhereBoolean.TRUE : WhereBoolean.FALSE;
        else
            return operator.equals(">") || operator.equals(">=") || operator.equals("!=") ? WhereBoolean.TRUE : WhereBoolean.FALSE;
    }

    @Override
    public String toString() {
        return "ComparisonPredicate{" +
                "leftForm=" + leftForm +
                ", rightForm=" + rightForm +
                ", operator='" + operator + '\'' +
                '}';
    }
}
