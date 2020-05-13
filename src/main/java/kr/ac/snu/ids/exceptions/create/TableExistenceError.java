package kr.ac.snu.ids.exceptions.create;

public class TableExistenceError extends CreateTableError {

    public TableExistenceError() {
        super("table with the same name already exists");
    }
}
