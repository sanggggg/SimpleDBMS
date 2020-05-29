package kr.ac.snu.ids.exceptions.where;

public class WhereIncomparableError extends WhereClauseError {
    public WhereIncomparableError() {
        super("try to compare incomparable values");
    }
}
