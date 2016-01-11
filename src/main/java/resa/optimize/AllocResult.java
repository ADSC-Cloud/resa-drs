package resa.optimize;

import java.util.Map;

/**
 * Created by ding on 14-4-29.
 */
public class AllocResult {

    //TODO: add expected QoS for both minReqOptAllocation and currOptAllocation
    //so that for later programme to optimize the rebalance behavior
    // (e.g. consider expected rebalance gain vs. cost)

    public static enum Status {
        INFEASIBLE, FEASIBLE
    }

    public final Status status;
    public final Map<String, Integer> minReqOptAllocation;
    public final Map<String, Integer> currOptAllocation;
    private Object context = null;

    public AllocResult(Status status, Map<String, Integer> minReqOptAllocation,
                       Map<String, Integer> currOptAllocation) {
        this.status = status;
        this.minReqOptAllocation = minReqOptAllocation;
        this.currOptAllocation = currOptAllocation;
    }

    public AllocResult(Status status, Map<String, Integer> currOptAllocation) {
        this(status, null, currOptAllocation);
    }

    public Object getContext() {
        return context;
    }

    public AllocResult setContext(Object context) {
        this.context = context;
        return this;
    }
}
