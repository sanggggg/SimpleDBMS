package kr.ac.snu.ids.query;

import kr.ac.snu.ids.query.predicate.BooleanCondition;

public class DeleteQuery {
    private String tableName;
    private BooleanCondition condition;

    private DeleteQuery(String tableName, BooleanCondition condition) {
        this.tableName = tableName;
        this.condition = condition;
    }

    public String getTableName() {
        return tableName;
    }

    public BooleanCondition getCondition() {
        return condition;
    }

    public static class Builder {
        private String tableName;
        private BooleanCondition condition;

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder setCondition(BooleanCondition condition) {
            this.condition = condition;
            return this;
        }

        public DeleteQuery create() {
            return new DeleteQuery(tableName, condition);
        }
    }
}
