package kr.ac.snu.ids.definition;

import java.io.Serializable;

public class DataTypeDefinition implements Serializable {
    private DataType dataType;
    private int charLength;

    public boolean comp(DataTypeDefinition obj) {

        return obj.getDataType().equals(this.getDataType()) && (this.getDataType() != DataType.CHARACTER || this.getCharLength() == obj.getCharLength());
    }

    public DataType getDataType() {
        return dataType;
    }

    public int getCharLength() {
        return charLength;
    }

    public DataTypeDefinition(DataType dataType, int charLength) {
        this.dataType = dataType;
        this.charLength = charLength;
    }

    public static class Builder {
        private DataType dataType;
        private int charLength = 0;

        public DataTypeDefinition.Builder setDataType(DataType dataType) {
            this.dataType = dataType;
            return this;
        }

        public DataTypeDefinition.Builder setCharLength(int charLength) {
            this.charLength = charLength;
            return this;
        }

        public DataTypeDefinition create() {
            return new DataTypeDefinition(dataType, charLength);
        }
    }

    @Override
    public String toString() {
        return "DataTypeDefinition{" +
                "dataType=" + dataType +
                ", charLength=" + charLength +
                '}';
    }
}
