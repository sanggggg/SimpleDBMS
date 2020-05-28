package kr.ac.snu.ids.exceptions.insert;

import kr.ac.snu.ids.exceptions.DefinitionError;

public abstract class InsertColumnError extends DefinitionError {
    static final private String prefix = "Insertion has failed: ";

    public InsertColumnError(String msg) {
        super(prefix + msg);
    }
}
