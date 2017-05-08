package resa.optimize;

import java.util.Map;

/**
 * Created by ding on 14-4-29.
 * Modified by Tom Fu on 8-May-2017
 *
 * in this version of modification, we extend the Status with two more states: "Overprovisioning" and "SHORTAGE"
 * The definition of both are not discussed here, refer to service model and decision maker
 */
public class AllocResult {

    //TODO: add expected QoS for both minReqOptAllocation and currOptAllocation, handled by a proper "Decision maker"
    //so that for later programme to optimize the rebalance behavior
    // (e.g. consider expected rebalance gain vs. cost)

    public static enum Status {
        OVERPROVISIONING, SHORTAGE, INFEASIBLE, FEASIBLE,
    }

    public final Status status;
    //    public final double minAchievableLatency;
    public final Map<String, Integer> minReqOptAllocation;
    public final Map<String, Integer> currOptAllocation;
    public final Map<String, Integer> kMaxOptAllocation;
    private Object context = null;

    public AllocResult(Status status, Map<String, Integer> minReqOptAllocation,
                       Map<String, Integer> currOptAllocation, Map<String, Integer> kMaxOptAllocation) {
        this.status = status;
        this.minReqOptAllocation = minReqOptAllocation;
        this.currOptAllocation = currOptAllocation;
        this.kMaxOptAllocation = kMaxOptAllocation;
    }

    public AllocResult(Status status, Map<String, Integer> minReqOptAllocation, Map<String, Integer> kMaxOptAllocation) {
        this(status, minReqOptAllocation, null, kMaxOptAllocation);
    }

    public Object getContext() {
        return context;
    }

    public AllocResult setContext(Object context) {
        this.context = context;
        return this;
    }
}
