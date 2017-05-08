package resa.optimize;

import java.util.Map;

/**
 * Created by Tom.fu on May-8-2017
 */
public interface ServiceModel {

    /**
     * A general interface for service models (e.g., MMK, GGK) for Storm 1.0.1, including two thresholds
     *
     * @param sourceNode
     * @param queueingNetwork
     * @param completeTimeMilliSecUpper i.e., T_{Max}
     * @param completeTimeMilliSecLower i.e., T_{Min}
     * @param currBoltAllocation
     * @param maxAvailable4Bolt         i.e., k_{Max}
     * @param currentUsedThreadByBolts  i.e., sum of number of executors in @currBoltAllocation
     * @param resourceUnit              used by get Min/MaxAllocation, each time the minimum number of executors will be added or removed
     * @return
     */
    AllocResult checkOptimized(
            SourceNode sourceNode, Map<String, ServiceNode> queueingNetwork,
            double completeTimeMilliSecUpper, double completeTimeMilliSecLower,
            Map<String, Integer> currBoltAllocation, int maxAvailable4Bolt,
            int currentUsedThreadByBolts, int resourceUnit);
}
