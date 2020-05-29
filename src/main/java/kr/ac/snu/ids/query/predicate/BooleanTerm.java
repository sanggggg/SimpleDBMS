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
    public boolean execute(TupleData tuple) {
        for (BooleanCondition factor : factors) {
            if (!factor.execute(tuple)) return false;
        }
        return true;
    }
}
