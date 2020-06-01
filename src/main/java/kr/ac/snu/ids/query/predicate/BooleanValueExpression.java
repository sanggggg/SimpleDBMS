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
    public WhereBoolean execute(TupleData tuple) {
        boolean undefined = false;
        for (BooleanCondition term : terms) {
            if (term.execute(tuple) == WhereBoolean.TRUE) return WhereBoolean.TRUE;
            else if (term.execute(tuple) == WhereBoolean.UNDEFINED) undefined = true;
        }

        if (undefined) return WhereBoolean.UNDEFINED;
        return WhereBoolean.FALSE;
    }

    @Override
    public String toString() {
        return "BooleanValueExpression{" +
                "terms=" + terms +
                '}';
    }
}
