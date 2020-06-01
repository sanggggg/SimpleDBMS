package kr.ac.snu.ids.query.predicate;

import kr.ac.snu.ids.db.TupleData;

import static kr.ac.snu.ids.query.predicate.WhereBoolean.UNDEFINED;

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
    public WhereBoolean execute(TupleData tuple) {
        switch (test.execute(tuple)) {
            case UNDEFINED:
                return UNDEFINED;
            case TRUE:
                return isNot ? WhereBoolean.FALSE : WhereBoolean.TRUE;
            case FALSE:
                return isNot ? WhereBoolean.TRUE : WhereBoolean.FALSE;
        }
        // Hmmm...
        return UNDEFINED;
    }
}
