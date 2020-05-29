package kr.ac.snu.ids.exceptions.where;

import kr.ac.snu.ids.exceptions.DefinitionError;

public abstract class WhereClauseError extends DefinitionError {
    static final private String prefix = "Where clause ";

    public WhereClauseError(String s) {
        super(prefix + s);
    }
}
