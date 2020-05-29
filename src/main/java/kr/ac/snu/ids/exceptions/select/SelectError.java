package kr.ac.snu.ids.exceptions.select;

import kr.ac.snu.ids.exceptions.DefinitionError;

public abstract class SelectError extends DefinitionError {
    static final private String prefix = "Selection has failed: ";

    public SelectError(String s) {
        super(prefix + s);
    }
}
