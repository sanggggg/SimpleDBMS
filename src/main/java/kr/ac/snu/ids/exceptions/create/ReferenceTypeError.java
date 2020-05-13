package kr.ac.snu.ids.exceptions.create;

public class ReferenceTypeError extends CreateTableError {
    public ReferenceTypeError() {
        super("foreign key references wrong type");
    }
}
