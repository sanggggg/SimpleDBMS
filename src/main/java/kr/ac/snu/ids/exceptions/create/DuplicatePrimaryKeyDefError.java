package kr.ac.snu.ids.exceptions.create;

public class DuplicatePrimaryKeyDefError extends CreateTableError {
    public DuplicatePrimaryKeyDefError() {
        super("primary key definition is duplicated");
    }
}
