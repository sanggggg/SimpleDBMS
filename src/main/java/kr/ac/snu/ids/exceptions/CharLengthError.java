package kr.ac.snu.ids.exceptions;

public class CharLengthError extends DefinitionError {
    public CharLengthError() {
        super("Char length should be over 0");
    }
}
