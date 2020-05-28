package kr.ac.snu.ids.exceptions.insert;

public class InsertReferentialIntegrityError extends InsertColumnError {
    public InsertReferentialIntegrityError() {
        super("Referential integrity violation");
    }
}
