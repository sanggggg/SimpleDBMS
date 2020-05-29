package kr.ac.snu.ids.exceptions.where;

public class WhereTableNotSpecifiedError extends WhereClauseError {
    public WhereTableNotSpecifiedError() {
        super("try to reference tables which are not specified");
    }
}
