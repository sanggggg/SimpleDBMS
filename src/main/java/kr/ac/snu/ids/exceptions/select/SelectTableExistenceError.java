package kr.ac.snu.ids.exceptions.select;

public class SelectTableExistenceError extends SelectError {
    public SelectTableExistenceError(String tableName) {
        super(String.format("'[%s]' dos not exist", tableName));
    }
}
