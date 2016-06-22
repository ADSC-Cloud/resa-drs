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
 * Modified by Tom Fu on 21-Dec-2015, for new DisruptQueue Implementation for Version after storm-core-0.10.0
 * Functions and Classes involving queue-related metrics in the current class will be affected:
 *   - calc()
 *   - LOG.info output
 *   - calculation of tupleCompleteRate and tupleProcRate is corrected. The duration metric is still necessary, only for this calculation.
 */
public class SimpleGeneralAllocCalculator extends AllocCalculator {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleGeneralAllocCalculator.class);
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

        ///TODO: Here we assume only one spout, how to extend to multiple spouts?
        ///TODO: here we assume only one running topology, how to extend to multiple running topologies?
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
                    double avgSendQLenHis = hisCar.getSendQueueResult().getQueueLength().getAvg();
                    double avgRecvQLenHis = hisCar.getRecvQueueResult().getQueueLength().getAvg();

                    ///TODO: there we multiply 1/2 for this particular implementation
                    ///double departRateHis = hisCar.getDepartureRatePerSec();
                    double departRateHis = hisCar.getSendQueueResult().getQueueArrivalRate().getAvg();
                    double tupleEmitRate = departRateHis * numberExecutor / 2.0;
                    double arrivalRateHis = hisCar.getRecvQueueResult().getQueueArrivalRate().getAvg();
                    double externalTupleArrivalRate = arrivalRateHis * numberExecutor;
                    double tupleEmitRateScv = hisCar.getSendQueueResult().getQueueArrivalRate().getScv();
                    double externalTupleArrivalScv = hisCar.getRecvQueueResult().getQueueArrivalRate().getScv();

                    double avgCompleteLatencyHis = hisCar.getCombinedCompletedLatency().getAvg();///unit is millisecond

                    long totalCompleteTupleCnt = hisCar.getCombinedCompletedLatency().getCount();
                    double totalDurationSecond = hisCar.getDurationSeconds();
                    double tupleCompleteRate = totalCompleteTupleCnt * numberExecutor / (totalDurationSecond * componentSampelRate);

                    LOG.info(String.format("Component(ID, eNum):(%s,%d), tupleFinCnt: %d, sumMeasuredDur: %.1f, hisSize: %d, sampleRate: %.1f, tupleFinRate: %.3f",
                            e.getKey(), numberExecutor, totalCompleteTupleCnt, totalDurationSecond, e.getValue().size(), componentSampelRate, tupleCompleteRate));
                    LOG.info(String.format("avgSQLenHis: %.1f, avgRQLenHis: %.1f, RQarrRateHis: %.4f, SQarrRateHis: %.4f",
                            avgSendQLenHis, avgRecvQLenHis, arrivalRateHis, departRateHis));
                    LOG.info(String.format("avgCompleteHis: %.4f, tupleEmitRate: %.4f, tupleEmitScv: %.4f, exArrivalRate: %.4f, exArrivalScv: %.4f",
                            avgCompleteLatencyHis, tupleEmitRate, tupleEmitRateScv, externalTupleArrivalRate, externalTupleArrivalScv));

                    ///TODO: shall we use externalTupleArrivalRate instead of tupleEmitRate???
                    return new SourceNode(avgCompleteLatencyHis, totalCompleteTupleCnt, hisCar.getDurationMilliseconds(), tupleEmitRate);
                }));

        SourceNode spInfo = spInfos.entrySet().stream().findFirst().get().getValue();

        Map<String, ServiceNode> queueingNetwork = boltHistoricalData.compHistoryResults.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                    BoltAggResult hisCar = AggResult.getHorizontalCombinedResult(new BoltAggResult(), e.getValue());
                    int numberExecutor = currAllocation.get(e.getKey());

                    double avgSendQLenHis = hisCar.getSendQueueResult().getQueueLength().getAvg();
                    double avgRecvQLenHis = hisCar.getRecvQueueResult().getQueueLength().getAvg();
                    double arrivalRateHis = hisCar.getRecvQueueResult().getQueueArrivalRate().getAvg();
                    double arrivalRateScv = hisCar.getRecvQueueResult().getQueueArrivalRate().getScv();
                    double avgServTimeHis = hisCar.getCombinedProcessedResult().getAvg();///unit is millisecond
                    double avgServTimeScv = hisCar.getCombinedProcessedResult().getScv();

                    long totalProcessTupleCnt = hisCar.getCombinedProcessedResult().getCount();
                    double totalDurationSecond = hisCar.getDurationSeconds();
                    double tupleProcessRate = totalProcessTupleCnt * numberExecutor / (totalDurationSecond * componentSampelRate);

                    double lambdaHis = arrivalRateHis * numberExecutor;
                    double muHis = 1000.0 / avgServTimeHis;
                    //TODO: when processed tuple count is very small (e.g. there is no input tuple, avgServTimeHis outputs zero),
                    // avgServTime becomes zero and mu becomes infinity, this will cause problematic SN.
                    double rhoHis = lambdaHis / muHis;

                    boolean sendQLenNormalHis = avgSendQLenHis < sendQSizeThresh;
                    boolean recvQlenNormalHis = avgRecvQLenHis < recvQSizeThresh;

                    ///TODO: here i2oRatio can be INFINITY, when there is no data sent from Spout.
                    ///TODO: here we shall deside whether to use external Arrival rate, or tupleLeaveRateOnSQ!!
                    ///TODO: major differences 1) when there is max-pending control, tupleLeaveRateOnSQ becomes the
                    ///TODO: the tupleEmit Rate, rather than the external tuple arrival rate (implicit load shading)
                    ///TODO: if use tupleLeaveRateOnSQ(), be careful to check if ACKing mechanism is on, i.e.,
                    ///TODO: there are ack tuples. othersize, devided by tow becomes meaningless.
                    ///TODO: shall we put this i2oRatio calculation here, or later to inside ServiceModel?
                    double i2oRatio = lambdaHis / spInfo.getTupleLeaveRateOnSQ();

                    LOG.info(String.format("Component(ID, eNum):(%s,%d), tupleProcCnt: %d, sumMeasuredDur: %.1f, hisSize: %d, sampleRate: %.1f, tupleProcRate: %.3f",
                            e.getKey(), numberExecutor, totalProcessTupleCnt, totalDurationSecond, e.getValue().size(), componentSampelRate, tupleProcessRate));
                    LOG.info(String.format("avgSQLenHis: %.1f, avgRQLenHis: %.1f, arrRateHis: %.4f, arrRateScv: %.4f, avgServTimeHis(ms): %.4f, avgServTimeScv: %.4f",
                            avgSendQLenHis, avgRecvQLenHis, arrivalRateHis, arrivalRateScv, avgServTimeHis, avgServTimeScv));
                    LOG.info(String.format("rhoHis: %.4f, lambdaHis: %.4f, muHis: %.4f, ratio: %.4f",
                            rhoHis, lambdaHis, muHis, i2oRatio));

                    return new ServiceNode(lambdaHis, muHis, ServiceNode.ServiceType.EXPONENTIAL, i2oRatio);
                }));
        int maxThreadAvailable4Bolt = maxAvailableExecutors - currAllocation.entrySet().stream()
                .filter(e -> rawTopology.get_spouts().containsKey(e.getKey()))
                .mapToInt(Map.Entry::getValue).sum();
        Map<String, Integer> boltAllocation = currAllocation.entrySet().stream()
                .filter(e -> rawTopology.get_bolts().containsKey(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        AllocResult allocResult = SimpleGeneralServiceModel.checkOptimized(queueingNetwork,
                spInfo.getRealLatencyMilliSec(), targetQoSMs, boltAllocation, maxThreadAvailable4Bolt);
        Map<String, Integer> retCurrAllocation = new HashMap<>(currAllocation);
        // merge the optimized decision into source allocation
        retCurrAllocation.putAll(allocResult.currOptAllocation);
        LOG.info(currAllocation + "-->" + retCurrAllocation);
        LOG.info("minReq: " + allocResult.minReqOptAllocation + ", status: " + allocResult.status);
        Map<String, Integer> retMinReqAllocation = null;
        if (allocResult.minReqOptAllocation != null) {
            retMinReqAllocation = new HashMap<>(currAllocation);
            // merge the optimized decision into source allocation
            retMinReqAllocation.putAll(allocResult.minReqOptAllocation);
        }
        Map<String, Object> ctx = new HashMap<>();
        ctx.put("latency", allocResult.getContext());
        ctx.put("spout", spInfo);
        ctx.put("bolt", queueingNetwork);
        return new AllocResult(allocResult.status, retMinReqAllocation, retCurrAllocation)
                .setContext(ctx);
    }

    @Override
    public void allocationChanged(Map<String, Integer> newAllocation) {
        super.allocationChanged(newAllocation);
        spoutHistoricalData.clear();
        boltHistoricalData.clear();
        currHistoryCursor = ConfigUtil.getInt(conf, ResaConfig.OPTIMIZE_WIN_HISTORY_SIZE_IGNORE, 0);
    }
}
