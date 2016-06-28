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
 * This is a simple implementation of decision maker
 * TODO: we need to develop a more robust solution (to prevent temporary oscillation)
 */
public class DefaultDecisionMaker implements DecisionMaker {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultDecisionMaker.class);

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

        public static String getTypeSting(int value){
            switch (value){
                case 0: return "CurrentOpt";
                case 1: return "MaxExecutorOpt";
                case 2: return "MinQoSOpt";
                default: return "unknown";
            }
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

        LOG.info("DefaultDecisionMaker.init(), stTime: " + startTimeMillis + ", minExpInteval: "
                + minExpectedIntervalMillis + ", rbType: " + RebalanceType.getTypeSting(rbTypeValue) + "(" + rbTypeValue + ")");
    }

    @Override
    public Map<String, Integer> make(AllocResult newAllocResult, Map<String, Integer> currAlloc) {

        long timeSpan = Math.max(0, System.currentTimeMillis() - startTimeMillis);
        if (newAllocResult == null) {
            LOG.info("DefaultDecisionMaker.make(), newAllocResult == null");
            return null;
        }

        if (timeSpan < minExpectedIntervalMillis) {
            /** if  timeSpan is not large enough, no rebalance will be triggered **/
            LOG.info("DefaultDecisionMaker.make(), timeSpan (" + timeSpan + ") < minExpectedIntervalMillis (" + minExpectedIntervalMillis + ")");
            return null;
        } else {
            if (rbTypeValue == RebalanceType.MaxExecutorOpt.getValue()) {
                if (newAllocResult.kMaxOptAllocation == null) {
                    LOG.info("DefaultDecisionMaker.make(), newAllocResult.kMaxOptAllocation == null, " +
                            "rebalance is not triggered, check whether the total resource is enough");
                } else {
                    LOG.info("DefaultDecisionMaker.make(), return newAllocResult.kMaxOptAllocation: " + newAllocResult.kMaxOptAllocation);
                }
                return newAllocResult.kMaxOptAllocation;

            } else if (rbTypeValue == RebalanceType.MinQoSOpt.getValue()) {
                if (newAllocResult.minReqOptAllocation == null) {
                    LOG.info("DefaultDecisionMaker.make(), newAllocResult.minReqOptAllocation == null, " +
                            "rebalance is not triggered, the targetQoS parameter is not feasible!");
                } else {
                    LOG.info("DefaultDecisionMaker.make(), return newAllocResult.minReqOptAllocation: " + newAllocResult.minReqOptAllocation);
                }
                return newAllocResult.minReqOptAllocation;
            } else {
                /** by default, we use CurrentOpt allocation **/
                if (newAllocResult.currOptAllocation == null) {
                    LOG.info("DefaultDecisionMaker.make(), newAllocResult.currOptAllocation == null, " +
                            "rebalance is not triggered, the current system may not be stable!");
                } else {
                    LOG.info("DefaultDecisionMaker.make(), return newAllocResult.currOptAllocation: " + newAllocResult.currOptAllocation);
                }
                return newAllocResult.currOptAllocation;
            }
        }
    }
}
