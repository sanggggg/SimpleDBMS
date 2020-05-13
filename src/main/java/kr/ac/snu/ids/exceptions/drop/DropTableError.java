package kr.ac.snu.ids.exceptions.drop;

import kr.ac.snu.ids.exceptions.DefinitionError;

public abstract class DropTableError extends DefinitionError {
    static final private String prefix = "Drop table has failed: ";

    public DropTableError(String msg) { super(prefix + msg); }
}

