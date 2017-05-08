package resa.drs;

import org.apache.storm.generated.StormTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.optimize.AllocResult;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;

import java.util.Map;

import static resa.util.ResaConfig.OPTIMIZE_INTERVAL;

/**
 * Created by ding on 14-6-20.
 */
public class BasicDecisionMaker implements DecisionMaker {

    private static final Logger LOG = LoggerFactory.getLogger(BasicDecisionMaker.class);


    /**
     * In future, if RebalanceType has more general usage, we will consider to move it to the ResaConfig class
     * case 1: return "MaxExecutorOpt";    ///always use up k_max number of executors
     * case 2: return "TwoThreshold";      ///ensure the expected sojourn time is between [T_min, T_max],
     * otherwise, it will automatically add/remove resources,
     * if amount of resources are appropriate, it checks wheter the current allocation is optimal or not.
     */
    enum RebalanceType {
        CurrentOpt(0), MaxExecutorOpt(1), TwoThreshold(2);
        private final int value;

        RebalanceType(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static String getTypeSting(int value) {
            switch (value) {
                case 0:
                    return "CurrentOpt";
                case 1:
                    return "MaxExecutorOpt";
                case 2:
                    return "TwoThreshold";
                default:
                    return "unknown";
            }
        }
    }

    long startTimeMillis;
    long minExpectedIntervalMillis;
    int rbTypeValue;

//    Map<String, Integer> kMaxAllocationRecord;
//    Map<String, Integer> currAllocationRecord;
//    Map<String, Integer> minRAllocationRecord;
//    Map<String, Integer> maxRAllocationRecord;
//
//    int kMaxAllocationRecordCount = 0;
//    int currAllocationRecordCount = 0;
//    int minRAllocationRecordCount = 0;
//    int maxRAllocationRecordCount = 0;

    @Override
    public void init(Map<String, Object> conf, StormTopology rawTopology) {
        startTimeMillis = System.currentTimeMillis();
        long calcIntervalSec = ConfigUtil.getInt(conf, OPTIMIZE_INTERVAL, 30);
        /** if OPTIMIZE_MIN_EXPECTED_REBALANCE_INTERVAL is not found in configuration files,
         * we use twice of OPTIMIZE_INTERVAL as default
         * here we -50 for synchronization purpose, this needs to be tested **/
        minExpectedIntervalMillis = ConfigUtil.getLong(conf, ResaConfig.OPTIMIZE_MIN_EXPECTED_REBALANCE_INTERVAL, calcIntervalSec * 2) * 1000 - 50;
        rbTypeValue = ConfigUtil.getInt(conf, ResaConfig.OPTIMIZE_REBALANCE_TYPE, RebalanceType.CurrentOpt.getValue());

        LOG.info("SimpleAdaptDecisionMaker.init(), stTime: " + startTimeMillis + ", minExpInteval: " + minExpectedIntervalMillis);
    }

    @Override
    public Map<String, Integer> make(AllocResult newAllocResult, Map<String, Integer> currAlloc) {

        long timeSpan = Math.max(0, System.currentTimeMillis() - startTimeMillis);
        if (newAllocResult == null) {
            LOG.info("SimpleAdaptDecisionMaker.make(), newAllocResult == null");
            return null;
        }
        if (timeSpan < minExpectedIntervalMillis) {
            /** if  timeSpan is not large enough, no rebalance will be triggered **/
            LOG.info("BasicDecisionMaker.make(), timeSpan (" + timeSpan + ") < minExpectedIntervalMillis (" + minExpectedIntervalMillis + ")");
            return null;
        }

        if (rbTypeValue == DefaultDecisionMaker.RebalanceType.MaxExecutorOpt.getValue()) {
            LOG.info("BasicDecisionMaker.make(), rbTypeValue == MaxExecutorOpt, rebalance is triggered with using maximum available resources");
            return newAllocResult.kMaxOptAllocation;
        } else {

            if (newAllocResult.status.equals(AllocResult.Status.OVERPROVISIONING)) {

                LOG.info("BasicDecisionMaker.make(), ewAllocResult.status == OVERPROVISIONING, rebalance is triggered with removing existing resources");
                return newAllocResult.minReqOptAllocation;

            } else if (newAllocResult.status.equals(AllocResult.Status.SHORTAGE)) {

                LOG.info("BasicDecisionMaker.make(), ewAllocResult.status == OVERPROVISIONING, rebalance is triggered with adding new resources");
                return newAllocResult.minReqOptAllocation;

            } else if (newAllocResult.status.equals(AllocResult.Status.INFEASIBLE)) {

                LOG.info("BasicDecisionMaker.make(), ewAllocResult.status == INFEASIBLE, rebalance is triggered with using maximum available resources");
                return newAllocResult.kMaxOptAllocation;

            } else if (newAllocResult.status.equals(AllocResult.Status.FEASIBLE)) {

                LOG.info("BasicDecisionMaker.make(), " +
                        "ewAllocResult.status == FEASIBLE, rebalance is triggered without adjusting current used resources, but re-allocation to optimal may be possible");
                ///return newAllocResult.currOptAllocation;
                return null;

            } else {
                throw new IllegalArgumentException("Illegal status: " + newAllocResult.status);
            }
        }
    }
}
