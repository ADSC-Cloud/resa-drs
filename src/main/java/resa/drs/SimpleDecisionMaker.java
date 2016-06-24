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
 * Created by Tom Fu on 24-6-2016.
 * This is a simple version of decision maker
 * <p>
 * functionality:
 * input:
 * output:
 */
public class SimpleDecisionMaker implements DecisionMaker {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleDecisionMaker.class);
    /**
     * In future, if RebalanceType has more general usage, we will consider to move it to the ResaConfig class
     */
    enum RebalanceType {
        CurrentOpt(0), MaxExecutorOpt(1), MinQoSOpt(2);
        private final int value;

        RebalanceType(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    long startTimeMillis;
    long minExpectedIntervalMillis;
    int rbTypeValue;

    @Override
    public void init(Map<String, Object> conf, StormTopology rawTopology) {
        startTimeMillis = System.currentTimeMillis();
        long calcIntervalSec = ConfigUtil.getInt(conf, OPTIMIZE_INTERVAL, 30);
        /** if OPTIMIZE_MIN_EXPECTED_REBALANCE_INTERVAL is not found in configuration files,
         * we use twice of OPTIMIZE_INTERVAL as default
         * here we -50 for synchronization purpose, this needs to be tested **/
        minExpectedIntervalMillis = ConfigUtil.getLong(conf, ResaConfig.OPTIMIZE_MIN_EXPECTED_REBALANCE_INTERVAL, calcIntervalSec * 2) * 1000 - 50;
        rbTypeValue = ConfigUtil.getInt(conf, ResaConfig.OPTIMIZE_REBALANCE_TYPE, RebalanceType.CurrentOpt.getValue());

        LOG.info("SimpleDecisionMaker.init, stTime: " + startTimeMillis + ", minExpInteval: " + minExpectedIntervalMillis + "rbTypeValue: " + rbTypeValue);
    }

    @Override
    public Map<String, Integer> make(AllocResult newAllocResult, Map<String, Integer> currAlloc) {

        long timeSpan = Math.max(0, System.currentTimeMillis() - startTimeMillis);
        if (timeSpan < minExpectedIntervalMillis) {
            /** if  timeSpan is not large enough, no rebalance will be triggered **/
            LOG.info("SimpleDecisionMaker.make, timeSpan: " + timeSpan + " < minExpectedIntervalMillis (" + minExpectedIntervalMillis + ")");
            return null;
        } else {
            if (rbTypeValue == RebalanceType.MaxExecutorOpt.getValue()) {
                if (newAllocResult.kMaxOptAllocation == null){
                    LOG.info("SimpleDecisionMaker.make, newAllocResult.kMaxOptAllocation == null, " +
                            "rebalance is not triggered, check whether the total resource is enough");
                }
                return newAllocResult.kMaxOptAllocation;

            } else if (rbTypeValue == RebalanceType.MinQoSOpt.getValue()) {
                if (newAllocResult.minReqOptAllocation == null){
                    LOG.info("SimpleDecisionMaker.make, newAllocResult.minReqOptAllocation == null, " +
                            "rebalance is not triggered, the targetQoS parameter is not feasible!");
                }
                return newAllocResult.minReqOptAllocation;
            } else {
                /** by default, we use CurrentOpt allocation **/
                if (newAllocResult.currOptAllocation == null){
                    LOG.info("SimpleDecisionMaker.make, newAllocResult.currOptAllocation == null, " +
                            "rebalance is not triggered, the current system may not be stable!");
                }
                return newAllocResult.currOptAllocation;
            }
        }
    }
}
