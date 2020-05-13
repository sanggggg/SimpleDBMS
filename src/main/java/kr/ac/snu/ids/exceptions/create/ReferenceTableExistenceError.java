package kr.ac.snu.ids.exceptions.create;

public class ReferenceTableExistenceError extends CreateTableError {

    public ReferenceTableExistenceError() {
        super("foreign key references non existing table");
    }
}
