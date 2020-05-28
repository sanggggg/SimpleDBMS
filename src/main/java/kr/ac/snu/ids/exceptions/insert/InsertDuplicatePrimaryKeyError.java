package kr.ac.snu.ids.exceptions.insert;

public class InsertDuplicatePrimaryKeyError extends InsertColumnError {
    public InsertDuplicatePrimaryKeyError() {
        super("Primary key duplication");
    }
}
