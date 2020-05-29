package kr.ac.snu.ids.exceptions.where;

public class WhereAmbiguousReferenceError extends WhereClauseError {
    public WhereAmbiguousReferenceError() {
        super("contains ambiguous reference");
    }
}
