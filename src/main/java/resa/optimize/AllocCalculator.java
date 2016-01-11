package resa.optimize;

import backtype.storm.generated.StormTopology;

import java.util.Map;

/**
 * Note: All the methods in this class will be invoked in the same thread, so it is not need to calc
 * them synchronized.
 * Created by ding on 14-4-30.
 */
public abstract class AllocCalculator {

    protected StormTopology rawTopology;
    protected Map<String, Object> conf;
    protected Map<String, Integer> currAllocation;

    /**
     * Called when a new instance was created.
     *
     * @param conf
     * @param rawTopology
     */
    public void init(Map<String, Object> conf, Map<String, Integer> currAllocation, StormTopology rawTopology) {
        this.conf = conf;
        this.rawTopology = rawTopology;
        this.currAllocation = currAllocation;
    }

    /**
     * This method will be invoked only when currAllocation was changed.
     *
     * @param newAllocation
     */
    public void allocationChanged(Map<String, Integer> newAllocation) {
        this.currAllocation = newAllocation;
    }

    public abstract AllocResult calc(Map<String, AggResult[]> executorAggResults, int maxAvailableExecutors);

}
