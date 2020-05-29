package kr.ac.snu.ids.query.predicate;

import kr.ac.snu.ids.db.TupleData;

public class BooleanFactor implements BooleanCondition {
    private Boolean isNot;
    private BooleanCondition test;

    public void setNot(Boolean not) {
        isNot = not;
    }

    public void setTest(BooleanCondition test) {
        this.test = test;
    }

    @Override
    public String toString() {
        return "BooleanFactor{" +
                "isNot=" + isNot +
                ", test=" + test +
                '}';
    }

    @Override
    public boolean execute(TupleData tuple) {
        return isNot ^ test.execute(tuple);
    }
}
