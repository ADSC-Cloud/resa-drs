package resa.optimize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.util.ConfigUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Created by Tom.fu on 23/4/2014.
 * Chain topology and not tuple split
 * TODO: the current implementation has adjustRatio, in future, we will design general regression model
 * TODO: or prediction method to replace this.
 *
 * TODO: sn.getI2oRatio() can be INFINITY, i.e., special case, there is no input data
 * TODO: this class needs improvement and redesign in the next version.
 */
public class SimpleGeneralServiceModel {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleGeneralServiceModel.class);

    /**
     * Like module A in our discussion
     *
     * @param components, the service node configuration, in this function, chain topology is assumed.
     * @param allocation, can be null input, in this case, directly return Infinity to indicator topology unstable
     * @return Double.MAX_VALUE when a) input allocation is null (i.e., system is unstable)
     * b) any one of the node is unstable (i.e., lambda/mu > 1, in which case, sn.estErlangT will be Double.MAX_VALUE)
     * else the validate estimated erlang service time.
     */
    public static double getErlangGeneralTopCompleteTime(Map<String, ServiceNode> components,
                                                         Map<String, Integer> allocation) {

        if (allocation == null) {
            return Double.MAX_VALUE;
        }
        double retVal = 0.0;
        for (Map.Entry<String, ServiceNode> e : components.entrySet()) {
            String cid = e.getKey();
            ServiceNode sn = e.getValue();
            Integer serverCount = allocation.get(cid);
            // Objects.requireNonNull(serverCount, "No allocation entry find for this component" + cid);
            double est = sn.estErlangT(serverCount);
            if (est < Double.MAX_VALUE) {
                retVal += (est * sn.getI2oRatio());
            } else {
                return Double.MAX_VALUE;
            }
        }
        return retVal;
    }

    public static double getErlangGeneralTopCompleteTimeMilliSec(Map<String, ServiceNode> components,
                                                                 Map<String, Integer> allocation) {
        double result = getErlangGeneralTopCompleteTime(components, allocation);
        return result < Double.MAX_VALUE ? (result * 1000.0) : Double.MAX_VALUE;
    }

    public static Map<String, Integer> getAllocation(Map<String, ServiceNode> components, Map<String, Object> para) {
        Map<String, Integer> retVal = new HashMap<>();

        components.forEach((cid, sn) -> {
            int curr = ConfigUtil.getInt(para, cid, 0);
            retVal.put(cid, curr);
        });
        return retVal;
    }

    public static boolean checkStable(Map<String, ServiceNode> components, Map<String, Integer> allocation) {
        return components.entrySet().stream().map(e -> e.getValue().isStable(allocation.get(e.getKey())))
                .allMatch(Boolean.TRUE::equals);
    }

    public static int getTotalMinRequirement(Map<String, ServiceNode> components) {
        return components.values().stream().mapToInt(ServiceNode::getMinReqServerCount).sum();
    }

    public static void printAllocation(Map<String, Integer> allocation) {
        if (allocation == null) {
            LOG.warn("Null allocation input -> system is unstable.");
        } else {
            LOG.info("allocation->" + allocation);
        }
    }

    /**
     * @param components
     * @param totalResourceCount
     * @return null if a) minReq of any component is Integer.MAX_VALUE (invalid parameter mu = 0.0)
     * b) total minReq can not be satisfied (total minReq > totalResourceCount)
     * otherwise, the Map data structure.
     */
    public static Map<String, Integer> suggestAllocationGeneralTop(Map<String, ServiceNode> components, int totalResourceCount) {
        Map<String, Integer> retVal = components.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> e.getValue().getMinReqServerCount()));
        int topMinReq = retVal.values().stream().mapToInt(Integer::intValue).sum();

        LOG.info("totalResourceCount: " + totalResourceCount + ", topMinReq: " + topMinReq);
        if (topMinReq <= totalResourceCount) {
            int remainCount = totalResourceCount - topMinReq;
            for (int i = 0; i < remainCount; i++) {
                double maxDiff = -1;
                String maxDiffCid = null;

                for (Map.Entry<String, ServiceNode> e : components.entrySet()) {
                    String cid = e.getKey();
                    ServiceNode sn = e.getValue();
                    int currentAllocated = retVal.get(cid);

                    double beforeAddT = sn.estErlangT(currentAllocated);
                    double afterAddT = sn.estErlangT(currentAllocated + 1);

                    double diff = (beforeAddT - afterAddT) * sn.getI2oRatio();
                    if (diff > maxDiff) {
                        maxDiff = diff;
                        maxDiffCid = cid;
                    }
                }
                if (maxDiffCid != null) {
                    int newAllocate = retVal.compute(maxDiffCid, (k, count) -> count + 1);
                    LOG.debug((i + 1) + " of " + remainCount + ", assigned to " + maxDiffCid + ", newAllocate: "
                            + newAllocate);
                } else {
                    LOG.debug("Null MaxDiffCid returned in " + (i + 1) + " of " + remainCount);
                    for (Map.Entry<String, ServiceNode> e : components.entrySet()) {
                        String cid = e.getKey();
                        ServiceNode sn = e.getValue();
                        int currentAllocated = retVal.get(cid);

                        double beforeAddT = sn.estErlangT(currentAllocated);
                        double afterAddT = sn.estErlangT(currentAllocated + 1);

                        LOG.debug(cid + ", currentAllocated: " + currentAllocated
                                + ", beforeAddT: " + beforeAddT
                                + ", afterAddT: " + afterAddT);
                    }
                    return retVal;
                }
            }
        } else {
            return null;
        }
        return retVal;
    }

    /**
     * Like Module A', input required QoS, output #threads required
     * Here we separate to two modules: first output allocation, then calculate total #threads included.
     * Caution all the computation involved is in second unit.
     *
     * @param components
     * @param maxAllowedCompleteTime, the unit here is second! consistent with function getErlangChainTopCompleteTime()
     * @param lowerBoundDelta,        this is to set the offset of the lowerBoundServiceTime, we require delta to be positive, and 0 as default.
     * @param adjRatio,               this is to adjust the estimated ErlangServiceTime to fit more closely to the real measured complte time
     * @return null if a) any service node is not in the valid state (mu = 0.0), this is not the case of rho > 1.0, just for checking mu
     * b) lowerBoundServiceTime > requiredQoS
     */
    public static Map<String, Integer> getMinReqServerAllocationGeneralTop(Map<String, ServiceNode> components,
                                                                           double maxAllowedCompleteTime,
                                                                           double lowerBoundDelta,
                                                                           double adjRatio,
                                                                           int maxAvailableExec) {
        double lowerBoundServiceTime = 0.0;
        int totalMinReq = 0;
        for (Map.Entry<String, ServiceNode> e : components.entrySet()) {
            double mu = e.getValue().getMu();
            ///caution, the unit should be millisecond
            lowerBoundServiceTime += (1.0 / mu);
            totalMinReq += e.getValue().getMinReqServerCount();
        }

        Map<String, Integer> currAllocation = null;
        if (lowerBoundServiceTime * adjRatio + lowerBoundDelta < maxAllowedCompleteTime) {
            double currTime;
            do {
                currAllocation = suggestAllocationGeneralTop(components, totalMinReq);
                currTime = getErlangGeneralTopCompleteTime(components, currAllocation) * adjRatio;

                LOG.debug(String.format("getMinReqServAllcQoS(ms): %.4f, rawCompleteTime(ms): %.4f, afterAdjust(ms): %.4f, totalMinReqQoS: %d",
                        maxAllowedCompleteTime * 1000.0, currTime * 1000.0 / adjRatio, currTime * 1000.0, totalMinReq));
                totalMinReq++;
                //check: we need to check totalMinReq to avoid infinite loop!
            } while (currTime > maxAllowedCompleteTime && totalMinReq <= maxAvailableExec);
        }
        return totalMinReq <= maxAvailableExec ? currAllocation : null;
    }

    public static Map<String, Integer> getMinReqServerAllocationGeneralTop(Map<String, ServiceNode> components,
                                                                           double maxAllowedCompleteTime,
                                                                           int maxAvailableExec) {
        return getMinReqServerAllocationGeneralTop(components, maxAllowedCompleteTime, 0.0, 1.0, maxAvailableExec);
    }

    public static Map<String, Integer> getMinReqServerAllocationGeneralTop(Map<String, ServiceNode> components,
                                                                           double maxAllowedCompleteTime,
                                                                           double adjRatio,
                                                                           int maxAvailableExec) {
        return getMinReqServerAllocationGeneralTop(components, maxAllowedCompleteTime, 0.0, adjRatio, maxAvailableExec);
    }

    public static int totalServerCountInvolved(Map<String, Integer> allocation) {
        return Objects.requireNonNull(allocation).values().stream().mapToInt(i -> i).sum();
    }


    /**
     * @param queueingNetwork
     * @param realLatencyMilliSec
     * @param targetQoSMilliSec
     * @param currBoltAllocation
     * @param maxAvailable4Bolt
     * @return status indicates whether the demanded QoS can be achieved or not
     * minReqAllocaiton, the minimum required resource (under optimized allocation) which can satisfy QoS
     * after: the optimized allocation under given maxAvailable4Bolt
     * TODO: the underestimation ratio shall be replaced by some sorts of prediction methods or regression models.
     */
    public static AllocResult checkOptimized(Map<String, ServiceNode> queueingNetwork, double realLatencyMilliSec,
                                             double targetQoSMilliSec, Map<String, Integer> currBoltAllocation,
                                             int maxAvailable4Bolt) {

        ///Caution about the time unit!, second is used in all the functions of calculation
        /// millisecond is used in the output display!
        double estimatedLatencyMilliSec = getErlangGeneralTopCompleteTimeMilliSec(queueingNetwork, currBoltAllocation);

        ///for better estimation, we remain (learn) this ratio, and assume that the estimated is always smaller than real.
        double underEstimateRatio = Math.max(1.0, realLatencyMilliSec / estimatedLatencyMilliSec);
        LOG.info(String.format("estLatency(ms): %.4f, realLatency(ms): %.4f, underEstRatio: %.4f",
                estimatedLatencyMilliSec, realLatencyMilliSec, underEstimateRatio));
        LOG.info("Find out minReqAllocation under QoS requirement.");
        Map<String, Integer> minReqAllocation = getMinReqServerAllocationGeneralTop(queueingNetwork,
                targetQoSMilliSec / 1000.0, underEstimateRatio, maxAvailable4Bolt * 2);
        AllocResult.Status status = AllocResult.Status.FEASIBLE;
        if (minReqAllocation == null) {
            status = AllocResult.Status.INFEASIBLE;
        }
        LOG.info("Find out best allocation given available executors.");
        Map<String, Integer> after = suggestAllocationGeneralTop(queueingNetwork, maxAvailable4Bolt);
        Map<String, Object> context = new HashMap<>();
        context.put("estLatency", estimatedLatencyMilliSec);
        context.put("realLatency", realLatencyMilliSec);
        context.put("underEstRatio", underEstimateRatio);
        return new AllocResult(status, minReqAllocation, after).setContext(context);
    }
}
