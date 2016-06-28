package resa.optimize;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by ding on 14-4-30.
 * Modified by Tom Fu on 21-Dec-2015, for new DisruptQueue Implementation for Version after storm-core-1.0.1
 * Functions and Classes involving queue-related metrics in the current class will be affected:
 *   - calc()
 *   - LOG.info output
 *   - calculation of tupleCompleteRate and tupleProcRate is corrected. The duration metric is still necessary, only for this calculation.
 */
public class MMKAllocCalculator extends AllocCalculator {
    private static final Logger LOG = LoggerFactory.getLogger(MMKAllocCalculator.class);
    private HistoricalCollectedData spoutHistoricalData;
    private HistoricalCollectedData boltHistoricalData;
    private int historySize;
    private int currHistoryCursor;

    @Override
    public void init(Map<String, Object> conf, Map<String, Integer> currAllocation, StormTopology rawTopology) {
        super.init(conf, currAllocation, rawTopology);
        ///The first (historySize - currHistoryCursor) window data will be ignored.
        historySize = ConfigUtil.getInt(conf, ResaConfig.OPTIMIZE_WIN_HISTORY_SIZE, 1);
        currHistoryCursor = ConfigUtil.getInt(conf, ResaConfig.OPTIMIZE_WIN_HISTORY_SIZE_IGNORE, 0);
        spoutHistoricalData = new HistoricalCollectedData(rawTopology, historySize);
        boltHistoricalData = new HistoricalCollectedData(rawTopology, historySize);
    }

    @Override
    public AllocResult calc(Map<String, AggResult[]> executorAggResults, int maxAvailableExecutors) {
        executorAggResults.entrySet().stream().filter(e -> rawTopology.get_spouts().containsKey(e.getKey()))
                .forEach(e -> spoutHistoricalData.putResult(e.getKey(), e.getValue()));
        executorAggResults.entrySet().stream().filter(e -> rawTopology.get_bolts().containsKey(e.getKey()))
                .forEach(e -> boltHistoricalData.putResult(e.getKey(), e.getValue()));
        // check history size. Ensure we have enough history data before we run the optimize function
        currHistoryCursor++;
        if (currHistoryCursor < historySize) {
            LOG.info("currHistoryCursor < historySize, curr: " + currHistoryCursor + ", Size: " + historySize
                    + ", DataHistorySize: "
                    + spoutHistoricalData.compHistoryResults.entrySet().stream().findFirst().get().getValue().size());
            return null;
        } else {
            currHistoryCursor = historySize;
        }

        ///TODO: Here we assume only one spout, plan to extend to multiple spouts in future
        ///TODO: here we assume only one running topology, plan to extend to multiple running topologies in future
        double targetQoSMs = ConfigUtil.getDouble(conf, ResaConfig.OPTIMIZE_SMD_QOS_MS, 5000.0);
        int maxSendQSize = ConfigUtil.getInt(conf, Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 1024);
        int maxRecvQSize = ConfigUtil.getInt(conf, Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 1024);
        double sendQSizeThresh = ConfigUtil.getDouble(conf, ResaConfig.OPTIMIZE_SMD_SEND_QUEUE_THRESH, 5.0);
        double recvQSizeThreshRatio = ConfigUtil.getDouble(conf, ResaConfig.OPTIMIZE_SMD_RECV_QUEUE_THRESH_RATIO, 0.6);
        double recvQSizeThresh = recvQSizeThreshRatio * maxRecvQSize;

        ///TODO: check how metrics are sampled in the current implementation.
        double componentSampelRate = ConfigUtil.getDouble(conf, ResaConfig.COMP_SAMPLE_RATE, 1.0);

        //Map<String, Map<String, Object>> queueMetric = new HashMap<>();
        Map<String, SourceNode> spInfos = spoutHistoricalData.compHistoryResults.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                    SpoutAggResult hisCar = AggResult.getHorizontalCombinedResult(new SpoutAggResult(), e.getValue());
                    int numberExecutor = currAllocation.get(e.getKey());
                    return new SourceNode(e.getKey(), numberExecutor, componentSampelRate, hisCar);
                }));

        SourceNode spInfo = spInfos.entrySet().stream().findFirst().get().getValue();

        Map<String, ServiceNode> queueingNetwork = boltHistoricalData.compHistoryResults.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                    BoltAggResult hisCar = AggResult.getHorizontalCombinedResult(new BoltAggResult(), e.getValue());
                    int numberExecutor = currAllocation.get(e.getKey());
                    ///TODO: here i2oRatio can be INFINITY, when there is no data sent from Spout.
                    ///TODO: here we shall deside whether to use external Arrival rate, or tupleLeaveRateOnSQ!!
                    ///TODO: major differences 1) when there is max-pending control, tupleLeaveRateOnSQ becomes the
                    ///TODO: the tupleEmit Rate, rather than the external tuple arrival rate (implicit load shading)
                    ///TODO: if use tupleLeaveRateOnSQ(), be careful to check if ACKing mechanism is on, i.e.,
                    ///TODO: there are ack tuples. othersize, devided by tow becomes meaningless.
                    ///TODO: shall we put this i2oRatio calculation here, or later to inside ServiceModel?
                    return new ServiceNode(e.getKey(), numberExecutor, componentSampelRate, hisCar, spInfo.getExArrivalRate());
                }));
        Map<String, Integer> boltAllocation = currAllocation.entrySet().stream()
                .filter(e -> rawTopology.get_bolts().containsKey(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        /** totalAvailableExecutors - spoutExecutors, currently, it is assumed that there is only one spout **/
        int maxThreadAvailable4Bolt = maxAvailableExecutors - currAllocation.entrySet().stream()
                .filter(e -> rawTopology.get_spouts().containsKey(e.getKey()))
                .mapToInt(Map.Entry::getValue).sum();

        int currentUsedThreadByBolts = currAllocation.entrySet().stream()
                .filter(e -> rawTopology.get_bolts().containsKey(e.getKey())).mapToInt(Map.Entry::getValue).sum();

        LOG.info("Run Optimization, tQos: " + targetQoSMs + ", currUsed: " + currentUsedThreadByBolts + ", kMax: " + maxThreadAvailable4Bolt + ", currAllo: " + currAllocation);
        AllocResult allocResult = ServiceModel.checkOptimized(
                spInfo, queueingNetwork, targetQoSMs, boltAllocation, maxThreadAvailable4Bolt, currentUsedThreadByBolts, ServiceModel.ServiceModelType.MMK);


        Map<String, Integer> retCurrAllocation = null;
        if (allocResult.currOptAllocation != null) {
            retCurrAllocation = new HashMap<>(currAllocation);
            retCurrAllocation.putAll(allocResult.currOptAllocation);
        }
        Map<String, Integer> retKMaxAllocation = null;
        if (allocResult.kMaxOptAllocation != null) {
            retKMaxAllocation = new HashMap<>(currAllocation);
            retKMaxAllocation.putAll(allocResult.kMaxOptAllocation);
        }
        Map<String, Integer> retMinReqAllocation = null;
        if (allocResult.minReqOptAllocation != null) {
            retMinReqAllocation = new HashMap<>(currAllocation);
            retMinReqAllocation.putAll(allocResult.minReqOptAllocation);
        }
        Map<String, Object> ctx = new HashMap<>();
        ctx.put("latency", allocResult.getContext());
        ctx.put("spout", spInfo);
        ctx.put("bolt", queueingNetwork);
        return new AllocResult(allocResult.status, retMinReqAllocation, retCurrAllocation, retKMaxAllocation).setContext(ctx);
    }

    @Override
    public void allocationChanged(Map<String, Integer> newAllocation) {
        super.allocationChanged(newAllocation);
        spoutHistoricalData.clear();
        boltHistoricalData.clear();
        currHistoryCursor = ConfigUtil.getInt(conf, ResaConfig.OPTIMIZE_WIN_HISTORY_SIZE_IGNORE, 0);
    }
}
