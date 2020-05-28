package kr.ac.snu.ids.exceptions.insert;

public class InsertTypeMismatchError extends InsertColumnError {
    public InsertTypeMismatchError() {
        super("Types are not matched");
    }
}
