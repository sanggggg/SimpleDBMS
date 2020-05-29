package kr.ac.snu.ids.query.predicate;

import kr.ac.snu.ids.db.TupleData;

public interface BooleanCondition {
    boolean execute(TupleData tuple);
}
