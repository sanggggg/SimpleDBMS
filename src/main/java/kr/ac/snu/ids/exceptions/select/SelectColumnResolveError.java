package kr.ac.snu.ids.exceptions.select;

public class SelectColumnResolveError extends SelectError {
    public SelectColumnResolveError(String colName) {
        super(String.format("fail to resolve '[%s]'", colName));
    }
}
