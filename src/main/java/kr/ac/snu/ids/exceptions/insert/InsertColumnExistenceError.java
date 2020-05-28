package kr.ac.snu.ids.exceptions.insert;

public class InsertColumnExistenceError extends InsertColumnError {
    public InsertColumnExistenceError(String colName) {
        super(String.format("'[%s]' does not exist", colName));
    }
}
