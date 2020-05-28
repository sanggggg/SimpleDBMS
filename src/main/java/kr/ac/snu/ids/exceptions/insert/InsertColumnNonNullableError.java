package kr.ac.snu.ids.exceptions.insert;

public class InsertColumnNonNullableError extends InsertColumnError {
    public InsertColumnNonNullableError(String colName) {
        super(String.format("'[%s]' is not nullable", colName));
    }
}
