package kr.ac.snu.ids.exceptions.create;

public class DuplicateColumnDefError extends CreateTableError {
    public DuplicateColumnDefError() {
        super("column definition is duplicated");
    }
}
