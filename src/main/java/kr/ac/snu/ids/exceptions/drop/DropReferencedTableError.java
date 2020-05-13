package kr.ac.snu.ids.exceptions.drop;

public class DropReferencedTableError extends DropTableError {
    public DropReferencedTableError(String tableName) {
        super(String.format("'%s' is referenced by other table", tableName));
    }
}
