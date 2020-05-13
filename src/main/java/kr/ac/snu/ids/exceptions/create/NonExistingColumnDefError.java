package kr.ac.snu.ids.exceptions.create;

public class NonExistingColumnDefError extends CreateTableError {
    public NonExistingColumnDefError(String colName) {
        super("'" + colName + "' does not exists in column definition");
    }
}
