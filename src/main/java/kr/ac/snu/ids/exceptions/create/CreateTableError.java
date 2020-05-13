package kr.ac.snu.ids.exceptions.create;

import kr.ac.snu.ids.exceptions.DefinitionError;

public abstract class CreateTableError extends DefinitionError {
    static final private String prefix = "Create table has failed: ";

    public CreateTableError(String msg) {
        super(prefix + msg);
    }
}
