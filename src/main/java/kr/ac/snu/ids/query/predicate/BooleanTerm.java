package kr.ac.snu.ids.query.predicate;

import kr.ac.snu.ids.db.TupleData;

import java.util.ArrayList;
import java.util.List;

public class BooleanTerm implements BooleanCondition {
    public List<BooleanCondition> factors = new ArrayList<>();

    public void addFactor(BooleanCondition factor) {
        factors.add(factor);
    }

    @Override
    public String toString() {
        return "BooleanTerm{" +
                "factors=" + factors +
                '}';
    }

    @Override
    public WhereBoolean execute(TupleData tuple) {
        WhereBoolean result;
        boolean undefined = false;
        for (BooleanCondition factor : factors) {
            result = factor.execute(tuple);
            if (result == WhereBoolean.FALSE) return WhereBoolean.FALSE;
            else if (result == WhereBoolean.UNDEFINED) undefined = true;

        }

        if (undefined) return WhereBoolean.UNDEFINED;
        return WhereBoolean.TRUE;
    }
}
