package kr.ac.snu.ids.db;

import kr.ac.snu.ids.definition.DataType;

public class ComparableValue {
    private DataType dataType;
    private String value;

    public DataType getDataType() {
        return dataType;
    }

    public String getValue() {
        return value;
    }

    public ComparableValue(DataType dataType, String value) {
        this.dataType = dataType;
        this.value = value;
    }

    @Override
    public String toString() {
        return "ComparableValue{" +
                "dataType=" + dataType +
                ", value='" + value + '\'' +
                '}';
    }
}
