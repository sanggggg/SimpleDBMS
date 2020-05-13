package kr.ac.snu.ids.exceptions.create;

public class ReferenceNonPrimaryKeyDefError extends CreateTableError {
    public ReferenceNonPrimaryKeyDefError() {
        super("foreign key references non primary key column");
    }
}
