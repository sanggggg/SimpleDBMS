package kr.ac.snu.ids.exceptions;

public class NoSuchTableError extends DefinitionError {
    public NoSuchTableError() {
        super("No such table");
    }
}
