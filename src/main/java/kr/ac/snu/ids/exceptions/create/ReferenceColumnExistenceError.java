package kr.ac.snu.ids.exceptions.create;

public class ReferenceColumnExistenceError extends CreateTableError {
    public ReferenceColumnExistenceError() {
        super("foreign key references non existing column");
    }
}
