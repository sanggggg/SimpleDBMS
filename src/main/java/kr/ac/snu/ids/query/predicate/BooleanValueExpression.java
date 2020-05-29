package kr.ac.snu.ids.query.predicate;

import kr.ac.snu.ids.db.TupleData;

import java.util.ArrayList;
import java.util.List;

public class BooleanValueExpression implements BooleanCondition {
    private List<BooleanCondition> terms = new ArrayList<>();

    public void addTerm(BooleanCondition term) {
        terms.add(term);
    }

    @Override
    public boolean execute(TupleData tuple) {
        for (BooleanCondition term : terms) {
            if (term.execute(tuple)) return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "BooleanValueExpression{" +
                "terms=" + terms +
                '}';
    }
}
