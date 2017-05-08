package resa.optimize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by Tom.fu on May-8-2017
 * TODO: to be merged into service model interface framework
 */
public class GGKServiceModel {

    private static final Logger LOG = LoggerFactory.getLogger(GGKServiceModel.class);

    /**
     * We assume the stability check for each node is done beforehand!
     * Jackson OQN assumes all the arrival and departure are iid and exponential
     *
     * Note, the return time unit is in Second!
     *
     * @param serviceNodes, the service node configuration, in this function, chain topology is assumed.
     * @param allocation,   the target allocation to be analyzed
     * @return here we should assume all the components are stable, the stability check shall be done outside this function
     */
    public static double getExpectedTotalSojournTimeForJacksonOQN(Map<String, ServiceNode> serviceNodes, Map<String, Integer> allocation) {

        double retVal = 0.0;
        for (Map.Entry<String, ServiceNode> e : serviceNodes.entrySet()) {
            String cid = e.getKey();
            ServiceNode serviceNode = e.getValue();
            int serverCount = allocation.get(cid).intValue();

            double avgSojournTime = sojournTime_MMK(serviceNode.getLambda(), serviceNode.getMu(), serverCount);
            retVal += (avgSojournTime * serviceNode.getRatio());
        }
        return retVal;
    }

    /**
     * We assume the stability check for each node is done beforehand!
     * Only assume iid with general distribution on interarrival times and service times
     * apply G/G/k service model
     * @param serviceNodes, the service node configuration, in this function, chain topology is assumed.
     * @param allocation,   the target allocation to be analyzed
     * @return here we should assume all the components are stable, the stability check shall be done outside this function
     */
    public static double getExpectedTotalSojournTimeForGeneralizedOQN_SimpleAppr(Map<String, ServiceNode> serviceNodes, Map<String, Integer> allocation) {

        double retVal = 0.0;
        for (Map.Entry<String, ServiceNode> e : serviceNodes.entrySet()) {
            String cid = e.getKey();
            ServiceNode serviceNode = e.getValue();
            int serverCount = allocation.get(cid).intValue();
            double avgSojournTime = sojournTime_GGK_SimpleAppr(
                    serviceNode.getLambda(), serviceNode.getInterArrivalScv(), serviceNode.getMu(), serviceNode.getScvServTimeHis(), serverCount);
            retVal += (avgSojournTime * serviceNode.getRatio());
        }
        return retVal;
    }

    public static double getExpectedTotalSojournTimeForGeneralizedOQN_ComplexAppr(Map<String, ServiceNode> serviceNodes, Map<String, Integer> allocation) {

        double retVal = 0.0;
        for (Map.Entry<String, ServiceNode> e : serviceNodes.entrySet()) {
            String cid = e.getKey();
            ServiceNode serviceNode = e.getValue();
            int serverCount = allocation.get(cid).intValue();
            double avgSojournTime = sojournTime_GGK_ComplexAppr(
                    serviceNode.getLambda(), serviceNode.getInterArrivalScv(), serviceNode.getMu(), serviceNode.getScvServTimeHis(), serverCount);
            retVal += (avgSojournTime * serviceNode.getRatio());
        }
        return retVal;
    }

    /**
     * @param serviceNodes
     * @param totalResourceCount
     * @return null if a) minReq of any component is Integer.MAX_VALUE (invalid parameter mu = 0.0)
     * b) total minReq can not be satisfied (total minReq > totalResourceCount)
     * otherwise, the Map data structure.
     */
    public static Map<String, Integer> suggestAllocationGeneralTopApplyMMK(Map<String, ServiceNode> serviceNodes, int totalResourceCount) {
        Map<String, Integer> retVal = serviceNodes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> getMinReqServerCount(e.getValue().getLambda(), e.getValue().getMu())));
        int topMinReq = retVal.values().stream().mapToInt(Integer::intValue).sum();

        LOG.debug("Apply M/M/K, resCnt: " + totalResourceCount + ", topMinReq: " + topMinReq);
        if (topMinReq <= totalResourceCount) {
            int remainCount = totalResourceCount - topMinReq;
            for (int i = 0; i < remainCount; i++) {
                double maxDiff = -1;
                String maxDiffCid = null;

                for (Map.Entry<String, ServiceNode> e : serviceNodes.entrySet()) {
                    String cid = e.getKey();
                    ServiceNode sn = e.getValue();
                    int currentAllocated = retVal.get(e.getKey());

                    double beforeAddT = sojournTime_MMK(sn.getLambda(), sn.getMu(), currentAllocated);
                    double afterAddT = sojournTime_MMK(sn.getLambda(), sn.getMu(), currentAllocated + 1);

                    double diff = (beforeAddT - afterAddT) * sn.getRatio();
                    if (diff > maxDiff) {
                        maxDiff = diff;
                        maxDiffCid = cid;
                    }
                }
                if (maxDiffCid != null) {
                    int newAllocate = retVal.compute(maxDiffCid, (k, count) -> count + 1);
                    LOG.debug((i + 1) + " of " + remainCount + ", assigned to " + maxDiffCid + ", newAllocate: " + newAllocate);
                } else {
                    LOG.debug("Null MaxDiffCid returned in " + (i + 1) + " of " + remainCount);
                    for (Map.Entry<String, ServiceNode> e : serviceNodes.entrySet()) {
                        String cid = e.getKey();
                        ServiceNode sn = e.getValue();
                        int currentAllocated = retVal.get(cid);

                        double beforeAddT = sojournTime_MMK(sn.getLambda(), sn.getMu(), currentAllocated);
                        double afterAddT = sojournTime_MMK(sn.getLambda(), sn.getMu(), currentAllocated + 1);

                        LOG.debug(cid + ", currentAllocated: " + currentAllocated
                                + ", beforeAddT: " + beforeAddT
                                + ", afterAddT: " + afterAddT);
                    }
                    return retVal;
                }
            }
        } else {
            LOG.info(String.format("topMinReq (%d) > totalResourceCount (%d)", topMinReq, totalResourceCount));
            return null;
        }
        return retVal;
    }

    public static Map<String, Integer> suggestAllocationGeneralTopApplyGGK_SimpleAppr(Map<String, ServiceNode> serviceNodes, int totalResourceCount) {
        Map<String, Integer> retVal = serviceNodes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> getMinReqServerCount(e.getValue().getLambda(), e.getValue().getMu())));
        int topMinReq = retVal.values().stream().mapToInt(Integer::intValue).sum();

        LOG.debug("Apply GGK_SimpleAppr, resCnt: " + totalResourceCount + ", topMinReq: " + topMinReq);
        if (topMinReq <= totalResourceCount) {
            int remainCount = totalResourceCount - topMinReq;
            for (int i = 0; i < remainCount; i++) {
                double maxDiff = -1;
                String maxDiffCid = null;

                for (Map.Entry<String, ServiceNode> e : serviceNodes.entrySet()) {
                    String cid = e.getKey();
                    ServiceNode sn = e.getValue();
                    int currentAllocated = retVal.get(e.getKey());

                    double beforeAddT = sojournTime_GGK_SimpleAppr(sn.getLambda(), sn.getInterArrivalScv(), sn.getMu(), sn.getScvServTimeHis(), currentAllocated);
                    double afterAddT = sojournTime_GGK_SimpleAppr(sn.getLambda(), sn.getInterArrivalScv(), sn.getMu(), sn.getScvServTimeHis(), currentAllocated + 1);

                    double diff = (beforeAddT - afterAddT) * sn.getRatio();
                    if (diff > maxDiff) {
                        maxDiff = diff;
                        maxDiffCid = cid;
                    }
                }
                if (maxDiffCid != null) {
                    int newAllocate = retVal.compute(maxDiffCid, (k, count) -> count + 1);
                    LOG.debug((i + 1) + " of " + remainCount + ", assigned to " + maxDiffCid + ", newAllocate: " + newAllocate);
                } else {
                    LOG.debug("Null MaxDiffCid returned in " + (i + 1) + " of " + remainCount);
                    for (Map.Entry<String, ServiceNode> e : serviceNodes.entrySet()) {
                        String cid = e.getKey();
                        ServiceNode sn = e.getValue();
                        int currentAllocated = retVal.get(cid);

                        double beforeAddT = sojournTime_GGK_SimpleAppr(sn.getLambda(), sn.getInterArrivalScv(), sn.getMu(), sn.getScvServTimeHis(), currentAllocated);
                        double afterAddT = sojournTime_GGK_SimpleAppr(sn.getLambda(), sn.getInterArrivalScv(), sn.getMu(), sn.getScvServTimeHis(), currentAllocated + 1);

                        LOG.debug(cid + ", currentAllocated: " + currentAllocated
                                + ", beforeAddT: " + beforeAddT
                                + ", afterAddT: " + afterAddT);
                    }
                    return retVal;
                }
            }
        } else {
            LOG.info(String.format("topMinReq (%d) > totalResourceCount (%d)", topMinReq, totalResourceCount));
            return null;
        }
        return retVal;
    }

    public static Map<String, Integer> suggestAllocationGeneralTopApplyGGK_ComplexAppr(Map<String, ServiceNode> serviceNodes, int totalResourceCount) {
        Map<String, Integer> retVal = serviceNodes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> getMinReqServerCount(e.getValue().getLambda(), e.getValue().getMu())));
        int topMinReq = retVal.values().stream().mapToInt(Integer::intValue).sum();

        LOG.debug("Apply GGK_ComplexAppr, resCnt: " + totalResourceCount + ", topMinReq: " + topMinReq);
        if (topMinReq <= totalResourceCount) {
            int remainCount = totalResourceCount - topMinReq;
            for (int i = 0; i < remainCount; i++) {
                double maxDiff = -1;
                String maxDiffCid = null;

                for (Map.Entry<String, ServiceNode> e : serviceNodes.entrySet()) {
                    String cid = e.getKey();
                    ServiceNode sn = e.getValue();
                    int currentAllocated = retVal.get(e.getKey());

                    double beforeAddT = sojournTime_GGK_ComplexAppr(sn.getLambda(), sn.getInterArrivalScv(), sn.getMu(), sn.getScvServTimeHis(), currentAllocated);
                    double afterAddT = sojournTime_GGK_ComplexAppr(sn.getLambda(), sn.getInterArrivalScv(), sn.getMu(), sn.getScvServTimeHis(), currentAllocated + 1);

                    double diff = (beforeAddT - afterAddT) * sn.getRatio();
                    if (diff > maxDiff) {
                        maxDiff = diff;
                        maxDiffCid = cid;
                    }
                }
                if (maxDiffCid != null) {
                    int newAllocate = retVal.compute(maxDiffCid, (k, count) -> count + 1);
                    LOG.debug((i + 1) + " of " + remainCount + ", assigned to " + maxDiffCid + ", newAllocate: " + newAllocate);
                } else {
                    LOG.debug("Null MaxDiffCid returned in " + (i + 1) + " of " + remainCount);
                    for (Map.Entry<String, ServiceNode> e : serviceNodes.entrySet()) {
                        String cid = e.getKey();
                        ServiceNode sn = e.getValue();
                        int currentAllocated = retVal.get(cid);

                        double beforeAddT = sojournTime_GGK_ComplexAppr(sn.getLambda(), sn.getInterArrivalScv(), sn.getMu(), sn.getScvServTimeHis(), currentAllocated);
                        double afterAddT = sojournTime_GGK_ComplexAppr(sn.getLambda(), sn.getInterArrivalScv(), sn.getMu(), sn.getScvServTimeHis(), currentAllocated + 1);

                        LOG.debug(cid + ", currentAllocated: " + currentAllocated
                                + ", beforeAddT: " + beforeAddT
                                + ", afterAddT: " + afterAddT);
                    }
                    return retVal;
                }
            }
        } else {
            LOG.info(String.format("topMinReq (%d) > totalResourceCount (%d)", topMinReq, totalResourceCount));
            return null;
        }
        return retVal;
    }

    /**
     * Here we separate to two modules: first output allocation, then calculate total #threads included.
     * Caution all the computation involved is in second unit.
     *
     * @param serviceNodes
     * @param maxAllowedCompleteTimeSeconds, the unit here is second! consistent with function getErlangChainTopCompleteTime()
     * @param lowerBoundDelta,        this is to set the offset of the lowerBoundServiceTime, we require delta to be positive, and 0 as default.
     * @param adjRatio,               this is to adjust the estimated ErlangServiceTime to fit more closely to the real measured complte time
     * @return null if a) any service node is not in the valid state (mu = 0.0), this is not the case of rho > 1.0, just for checking mu
     * b) lowerBoundServiceTime > requiredQoS
     */
    public static Map<String, Integer> getMinReqServerAllocationGeneralTopApplyMMK(Map<String, ServiceNode> serviceNodes,
                                                                           double maxAllowedCompleteTimeSeconds,
                                                                           double lowerBoundDelta,
                                                                           double adjRatio,
                                                                           int maxAvailableExec) {
        double lowerBoundServiceTime = 0.0;
        int totalMinReq = 0;
        for (Map.Entry<String, ServiceNode> e : serviceNodes.entrySet()) {
            double lambda = e.getValue().getLambda();
            double mu = e.getValue().getMu();
            ///caution, the unit should be millisecond
            lowerBoundServiceTime += (1.0 / mu);
            totalMinReq += getMinReqServerCount(lambda, mu);
        }

        Map<String, Integer> currAllocation = null;
        if (lowerBoundServiceTime * adjRatio + lowerBoundDelta < maxAllowedCompleteTimeSeconds) {
            double currTime;
            do {
                currAllocation = suggestAllocationGeneralTopApplyMMK(serviceNodes, totalMinReq);
                currTime = getExpectedTotalSojournTimeForJacksonOQN(serviceNodes, currAllocation) * adjRatio;

                LOG.debug(String.format("getMinReqServAllcQoS(ms): %.4f, rawCompleteTime(ms): %.4f, afterAdjust(ms): %.4f, totalMinReqQoS: %d",
                        maxAllowedCompleteTimeSeconds * 1000.0, currTime * 1000.0 / adjRatio, currTime * 1000.0, totalMinReq));

                totalMinReq++;
                //check: we need to check totalMinReq to avoid infinite loop!
            } while (currTime > maxAllowedCompleteTimeSeconds && totalMinReq <= maxAvailableExec);
        }
        return totalMinReq <= maxAvailableExec ? currAllocation : null;
    }

    public static Map<String, Integer> getMinReqServerAllocationGeneralTopApplyMMK(Map<String, ServiceNode> serviceNodes,
                                                                           double maxAllowedCompleteTime,
                                                                           double adjRatio,
                                                                           int maxAvailableExec) {
        return getMinReqServerAllocationGeneralTopApplyMMK(serviceNodes, maxAllowedCompleteTime, 0.0, adjRatio, maxAvailableExec);
    }

    /**
     * @param queueingNetwork
     * @param queueingNetwork
     * @param targetQoSMilliSec
     * @param currBoltAllocation
     * @param maxAvailable4Bolt
     * @param currentUsedThreadByBolts
     * @return status indicates whether the demanded QoS can be achieved or not
     * minReqAllocaiton, the minimum required resource (under optimized allocation) which can satisfy QoS
     * after: the optimized allocation under given maxAvailable4Bolt
     */
    public static AllocResult checkOptimized_MMK(SourceNode sourceNode, Map<String, ServiceNode> queueingNetwork,
                                                 double targetQoSMilliSec, Map<String, Integer> currBoltAllocation,
                                                 int maxAvailable4Bolt, int currentUsedThreadByBolts) {

        ///Todo we need to do the stability check first here!
        ///Caution about the time unit!, second is used in all the functions of calculation
        /// millisecond is used in the output display!
        double realLatencyMilliSeconds = sourceNode.getRealLatencyMilliSeconds();
        double estTotalSojournTimeMilliSec_MMK = 1000.0 * getExpectedTotalSojournTimeForJacksonOQN(queueingNetwork, currBoltAllocation);
        ///for better estimation, we remain (learn) this ratio, and assume that the estimated is always smaller than real.
        double underEstimateRatio = Math.max(1.0, realLatencyMilliSeconds / estTotalSojournTimeMilliSec_MMK);
        ///relativeError (rE)
        double relativeError = Math.abs(realLatencyMilliSeconds - estTotalSojournTimeMilliSec_MMK) * 100.0 / realLatencyMilliSeconds;

        LOG.info(String.format("realLatency(ms): %.4f, estMMK: %.4f, urMMK: %.4f, reMMK: %.4f",
                realLatencyMilliSeconds, estTotalSojournTimeMilliSec_MMK, underEstimateRatio, relativeError));

        LOG.debug("Find out minReqAllocation under QoS requirement.");
        Map<String, Integer> minReqAllocation = getMinReqServerAllocationGeneralTopApplyMMK(queueingNetwork,
                targetQoSMilliSec / 1000.0, underEstimateRatio, maxAvailable4Bolt * 2);
        AllocResult.Status status = AllocResult.Status.FEASIBLE;
        if (minReqAllocation == null) {
            status = AllocResult.Status.INFEASIBLE;
        }
        LOG.debug("Find out best allocation given available executors.");
        Map<String, Integer> kMaxOptAllocation = suggestAllocationGeneralTopApplyMMK(queueingNetwork, maxAvailable4Bolt);
        Map<String, Integer> currOptAllocation = suggestAllocationGeneralTopApplyMMK(queueingNetwork, currentUsedThreadByBolts);
        Map<String, Object> context = new HashMap<>();
        context.put("realLatency", realLatencyMilliSeconds);
        context.put("estMMK", estTotalSojournTimeMilliSec_MMK);
        context.put("urMMK", underEstimateRatio);
        context.put("reMMK", relativeError);
        return new AllocResult(status, minReqAllocation, currOptAllocation, kMaxOptAllocation).setContext(context);
    }

    public static AllocResult checkOptimized_GGK_SimpleAppr(SourceNode sourceNode, Map<String, ServiceNode> queueingNetwork,
                                                            double targetQoSMilliSec, Map<String, Integer> currBoltAllocation,
                                                            int maxAvailable4Bolt, int currentUsedThreadByBolts) {

        ///Todo we need to do the stability check first here!
        ///TODO so far, we still use getMinReqServerAllocationGeneralTopApplyMMK to get minReqValue for temp use
        ///Caution about the time unit!, second is used in all the functions of calculation
        /// millisecond is used in the output display!
        double realLatencyMilliSeconds = sourceNode.getRealLatencyMilliSeconds();
        double estTotalSojournTimeMilliSec_GGK_SAppr = 1000.0 * getExpectedTotalSojournTimeForGeneralizedOQN_SimpleAppr(queueingNetwork, currBoltAllocation);
        ///for better estimation, we remain (learn) this ratio, and assume that the estimated is always smaller than real.
        double underEstimateRatio = Math.max(1.0, realLatencyMilliSeconds / estTotalSojournTimeMilliSec_GGK_SAppr);
        ///relativeError (rE)
        double relativeError = Math.abs(realLatencyMilliSeconds - estTotalSojournTimeMilliSec_GGK_SAppr) * 100.0 / realLatencyMilliSeconds;

        LOG.info(String.format("realLatency(ms): %.4f, estGGKSAppr: %.4f, urGGKSAppr: %.4f, reGGKSAppr: %.4f",
                realLatencyMilliSeconds, estTotalSojournTimeMilliSec_GGK_SAppr, underEstimateRatio, relativeError));

        LOG.debug("Find out minReqAllocation under QoS requirement.");
        Map<String, Integer> minReqAllocation = getMinReqServerAllocationGeneralTopApplyMMK(queueingNetwork,
                targetQoSMilliSec / 1000.0, underEstimateRatio, maxAvailable4Bolt * 2);
        AllocResult.Status status = AllocResult.Status.FEASIBLE;
        if (minReqAllocation == null) {
            status = AllocResult.Status.INFEASIBLE;
        }
        LOG.debug("Find out best allocation given available executors.");
        Map<String, Integer> kMaxOptAllocation = suggestAllocationGeneralTopApplyGGK_SimpleAppr(queueingNetwork, maxAvailable4Bolt);
        Map<String, Integer> currOptAllocation = suggestAllocationGeneralTopApplyGGK_SimpleAppr(queueingNetwork, currentUsedThreadByBolts);
        Map<String, Object> context = new HashMap<>();
        context.put("realLatency", realLatencyMilliSeconds);
        context.put("estGGKSAppr", estTotalSojournTimeMilliSec_GGK_SAppr);
        context.put("urGGKSAppr", underEstimateRatio);
        context.put("reGGKSAppr", relativeError);
        return new AllocResult(status, minReqAllocation, currOptAllocation, kMaxOptAllocation).setContext(context);
    }

    public static AllocResult checkOptimized_GGK_ComplexAppr(SourceNode sourceNode, Map<String, ServiceNode> queueingNetwork,
                                                             double targetQoSMilliSec, Map<String, Integer> currBoltAllocation,
                                                             int maxAvailable4Bolt, int currentUsedThreadByBolts) {

        ///Todo we need to do the stability check first here!
        ///TODO so far, we still use getMinReqServerAllocationGeneralTopApplyMMK to get minReqValue for temp use
        ///Caution about the time unit!, second is used in all the functions of calculation
        /// millisecond is used in the output display!
        double realLatencyMilliSeconds = sourceNode.getRealLatencyMilliSeconds();
        double estTotalSojournTimeMilliSec_GGK_CAppr = 1000.0 * getExpectedTotalSojournTimeForGeneralizedOQN_ComplexAppr(queueingNetwork, currBoltAllocation);
        ///for better estimation, we remain (learn) this ratio, and assume that the estimated is always smaller than real.
        double underEstimateRatio = Math.max(1.0, realLatencyMilliSeconds / estTotalSojournTimeMilliSec_GGK_CAppr);
        ///relativeError (rE)
        double relativeError = Math.abs(realLatencyMilliSeconds - estTotalSojournTimeMilliSec_GGK_CAppr) * 100.0 / realLatencyMilliSeconds;

        LOG.info(String.format("realLatency(ms): %.4f, estGGKCAppr: %.4f, urGGKCAppr: %.4f, reGGKCAppr: %.4f",
                realLatencyMilliSeconds, estTotalSojournTimeMilliSec_GGK_CAppr, underEstimateRatio, relativeError));

        LOG.debug("Find out minReqAllocation under QoS requirement.");
        Map<String, Integer> minReqAllocation = getMinReqServerAllocationGeneralTopApplyMMK(queueingNetwork,
                targetQoSMilliSec / 1000.0, underEstimateRatio, maxAvailable4Bolt * 2);
        AllocResult.Status status = AllocResult.Status.FEASIBLE;
        if (minReqAllocation == null) {
            status = AllocResult.Status.INFEASIBLE;
        }
        LOG.debug("Find out best allocation given available executors.");
        Map<String, Integer> kMaxOptAllocation = suggestAllocationGeneralTopApplyGGK_ComplexAppr(queueingNetwork, maxAvailable4Bolt);
        Map<String, Integer> currOptAllocation = suggestAllocationGeneralTopApplyGGK_ComplexAppr(queueingNetwork, currentUsedThreadByBolts);
        Map<String, Object> context = new HashMap<>();
        context.put("realLatency", realLatencyMilliSeconds);
        context.put("estGGKCAppr", estTotalSojournTimeMilliSec_GGK_CAppr);
        context.put("urGGKCAppr", underEstimateRatio);
        context.put("reGGKCAppr", relativeError);
        return new AllocResult(status, minReqAllocation, currOptAllocation, kMaxOptAllocation).setContext(context);
    }


    /**
     * Currently we do not provide GGK model for Storm 1.0.1
     * @param sourceNode
     * @param queueingNetwork
     * @param targetQoSMilliSec
     * @param currBoltAllocation
     * @param maxAvailable4Bolt
     * @param currentUsedThreadByBolts
     * @return
     */
    public static AllocResult checkOptimized(SourceNode sourceNode, Map<String, ServiceNode> queueingNetwork,
                                             double targetQoSMilliSec, Map<String, Integer> currBoltAllocation,
                                             int maxAvailable4Bolt, int currentUsedThreadByBolts){

//        AllocResult allocResult[] = new AllocResult[ServiceModelType.totalTypeCount];
//
//        allocResult[0] = checkOptimized_MMK(sourceNode, queueingNetwork, targetQoSMilliSec, currBoltAllocation, maxAvailable4Bolt, currentUsedThreadByBolts);
//        LOG.info("MMK,  minReqAllo: " + allocResult[0].minReqOptAllocation + ", minReqStatus: " + allocResult[0].status);
//        LOG.info("MMK, currOptAllo: " + allocResult[0].currOptAllocation);
//        LOG.info("MMK, kMaxOptAllo: " + allocResult[0].kMaxOptAllocation);
//        allocResult[1] = checkOptimized_GGK_SimpleAppr(sourceNode, queueingNetwork, targetQoSMilliSec, currBoltAllocation, maxAvailable4Bolt, currentUsedThreadByBolts);
//        LOG.info("GGKSAppr,  minReqAllo: " + allocResult[1].minReqOptAllocation + ", minReqStatus: " + allocResult[1].status);
//        LOG.info("GGKSAppr, currOptAllo: " + allocResult[1].currOptAllocation);
//        LOG.info("GGKSAppr, kMaxOptAllo: " + allocResult[1].kMaxOptAllocation);
//        allocResult[2] = checkOptimized_GGK_ComplexAppr(sourceNode, queueingNetwork, targetQoSMilliSec, currBoltAllocation, maxAvailable4Bolt, currentUsedThreadByBolts);
//        LOG.info("GGKCAppr,  minReqAllo: " + allocResult[2].minReqOptAllocation + ", minReqStatus: " + allocResult[2].status);
//        LOG.info("GGKCAppr, currOptAllo: " + allocResult[2].currOptAllocation);
//        LOG.info("GGKCAppr, kMaxOptAllo: " + allocResult[2].kMaxOptAllocation);

//        Map<String, Object> context = new HashMap<>();
//        context.putAll((Map<String, Object>)allocResult[0].getContext());
//        context.putAll((Map<String, Object>)allocResult[1].getContext());
//        context.putAll((Map<String, Object>)allocResult[2].getContext());
//
//        return allocResult[retAlloType.getValue()].setContext(context);

//        return allocResult[retAlloType.getValue()];
        AllocResult allocResult = checkOptimized_MMK(sourceNode, queueingNetwork, targetQoSMilliSec, currBoltAllocation, maxAvailable4Bolt, currentUsedThreadByBolts);
        LOG.info("MMK,  minReqAllo: " + allocResult.minReqOptAllocation + ", minReqStatus: " + allocResult.status);
        LOG.info("MMK, currOptAllo: " + allocResult.currOptAllocation);
        LOG.info("MMK, kMaxOptAllo: " + allocResult.kMaxOptAllocation);

        return allocResult;
    }

    /**
     * if mu = 0.0 or serverCount not positive, then rho is not defined, we consider it as the unstable case (represented by Double.MAX_VALUE)
     * otherwise, return the calculation results. Leave the interpretation to the calling function, like isStable();
     *
     * @param lambda
     * @param mu
     * @param serverCount
     * @return
     */
    private static double calcRho(double lambda, double mu, int serverCount) {
        return (mu > 0.0 && serverCount > 0) ? lambda / (mu * (double) serverCount) : Double.MAX_VALUE;
    }

    /**
     * First call getRho,
     * then determine when rho is validate, i.e., rho < 1.0
     * otherwise return unstable (FALSE)
     *
     * @param lambda
     * @param mu
     * @param serverCount
     * @return
     */
    public static boolean isStable(double lambda, double mu, int serverCount) {
        return calcRho(lambda, mu, serverCount) < 1.0;
    }

    private static double calcRhoSingleServer(double lambda, double mu) {
        return calcRho(lambda, mu, 1);
    }

    public static int getMinReqServerCount(double lambda, double mu) {
        return (int) (lambda / mu) + 1;
    }


    /**
     * we assume the stability check is done before calling this function
     * The total sojournTime of an MMK queue is the sum of queueing time and expected service time (1.0 / mu).
     *
     * @param lambda,     average arrival rate
     * @param mu,         average execute rate
     * @param serverCount
     * @return
     */
    public static double sojournTime_MMK(double lambda, double mu, int serverCount) {
        return avgQueueingTime_MMK(lambda, mu, serverCount) + 1.0 / mu;
    }

    /**
     * we assume the stability check is done before calling this function
     * This is a simple version of approximation on average sojourn time for the G/G/K queue
     * E(Wq) = (caj+csj)/2 E(Wq(M/M/K)) --> Eq. 62 of survey paper on OQN by Gabriel Bitran
     *
     * @param lambda
     * @param scvArrival, scv of inter-arrival times
     * @param mu
     * @param scvService, scv of service times
     * @param serverCount
     * @return
     */
    public static double sojournTime_GGK_SimpleAppr(double lambda, double scvArrival, double mu, double scvService, int serverCount) {
        double adjust = (scvArrival + scvService) / 2.0;
        return avgQueueingTime_MMK(lambda, mu, serverCount) * adjust + 1.0 / mu;
    }

    /**
     * we assume the stability check is done before calling this function
     * This is a more complex and accurate version of approximation on average sojourn time for the G/G/K queue
     * E(Wq) = \Phi(rho, caj, csj, k) * (caj+csj)/2 E(Wq(M/M/K)) --> Appendix 1 of "Machine allocation algorithms for job shop manufacturing", van Vliet and R. Kan
     *
     * @param lambda
     * @param scvArrival, scv of inter-arrival times
     * @param mu
     * @param scvService, scv of service times
     * @param serverCount
     * @return
     */
    public static double sojournTime_GGK_ComplexAppr(double lambda, double scvArrival, double mu, double scvService, int serverCount) {

        double k = serverCount;
        double rho = lambda / (mu * k);
        double gamma = (1.0 - rho) * (k - 1.0) * (Math.sqrt(4.0 + 5.0 * k) - 2.0) / (16.0 * k * rho);
        gamma = Math.min(0.24, gamma);

        double f1 = 1.0 + gamma;
        double f2 = 1.0 - 4.0 * gamma;
        double f3 = f2 * Math.exp(-2.0 * (1.0 - rho) / (3.0 * rho));
        double f4 = Math.min(1.0, (f1 + f3) / 2.0);

        double adjust = (scvArrival + scvService) / 2.0;
        double Psi = adjust < 1.0 ? Math.pow(f4, 2.0 - 2.0 * adjust) : 1.0;

//        double Phy = 0.0;
//        if (scvArrival >= scvService) {
//            Phy = f1 * (scvArrival - scvService) / (scvArrival - 0.75 * scvService)
//                    + Psi * scvService / (4.0 * scvArrival - 3.0 * scvService);
//        } else {
//            Phy = f3 * 0.5 * (scvService - scvArrival) / (scvArrival + scvService)
//                    + Psi * 0.5 * (scvService + 3.0 * scvArrival) / (scvArrival + scvService);
//        }
//      simplified as:
        double Phy = (scvArrival >= scvService)
                ? (f1 * (scvArrival - scvService) / (scvArrival - 0.75 * scvService)
                    + Psi * scvService / (4.0 * scvArrival - 3.0 * scvService))
                : (f3 * 0.5 * (scvService - scvArrival) / (scvArrival + scvService)
                    + Psi * 0.5 * (scvService + 3.0 * scvArrival) / (scvArrival + scvService));

        return avgQueueingTime_MMK(lambda, mu, serverCount) * adjust * Phy + 1.0 / mu;
    }

    /**
     * we assume the stability check is done before calling this function
     * This is a standard erlang-C formula
     *
     * @param lambda
     * @param mu
     * @param serverCount
     * @return
     */
    public static double avgQueueingTime_MMK(double lambda, double mu, int serverCount) {
        double r = lambda / (mu * (double) serverCount);
        double kr = lambda / mu;

        double phi0_p1 = 1.0;
        for (int i = 1; i < serverCount; i++) {
            double a = Math.pow(kr, i);
            double b = (double) factorial(i);
            phi0_p1 += (a / b);
        }

        double phi0_p2_nor = Math.pow(kr, serverCount);
        double phi0_p2_denor = (1.0 - r) * (double) (factorial(serverCount));
        double phi0_p2 = phi0_p2_nor / phi0_p2_denor;

        double phi0 = 1.0 / (phi0_p1 + phi0_p2);

        double pWait = phi0_p2 * phi0;

        double waitingTime = pWait * r / ((1.0 - r) * lambda);

        return waitingTime;
    }


    public static double sojournTime_MM1(double lambda, double mu) {
        return 1.0 / (mu - lambda);
    }

    private static int factorial(int n) {
        if (n < 0) {
            throw new IllegalArgumentException("Attention, negative input is not allowed: " + n);
        } else if (n == 0) {
            return 1;
        } else {
            int ret = 1;
            for (int i = 2; i <= n; i++) {
                ret = ret * i;
            }
            return ret;
        }
    }
}
