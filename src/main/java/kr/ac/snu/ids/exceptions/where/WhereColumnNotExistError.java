package kr.ac.snu.ids.exceptions.where;

public class WhereColumnNotExistError extends WhereClauseError {
    public WhereColumnNotExistError() {
        super("try to reference non existing column");
    }
}
