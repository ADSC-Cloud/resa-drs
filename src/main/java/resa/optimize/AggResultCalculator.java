package resa.optimize;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.ExecutorDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.metrics.MeasuredData;
import resa.metrics.MetricNames;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by ding on 14-3-4.
 * Note:
 * Recv-Queue arrival count includes ack for each message
 * When calculate sum and average, need to adjust (sum - #message, average - 1) for accurate value.
 *
 * Modified by Tom Fu on 21-Dec-2015, for new DisruptQueue Implementation for Version after storm-core-0.10.0
 * Functions and Classes involving queue-related metrics in the current class will be affected:
 *  - parseQueueResult()
 *  - AggResult parse(MeasuredData measuredData, AggResult dest)
 */
public class AggResultCalculator {

    private static final Logger LOG = LoggerFactory.getLogger(AggResultCalculator.class);

    protected Iterable<MeasuredData> dataStream;
    private StormTopology rawTopo;
    private final Map<Integer, AggResult> task2ExecutorResult = new HashMap<>();
    private final Map<String, AggResult[]> comp2ExecutorResults = new HashMap<>();
    private final Set<Integer> firstTasks;

    public AggResultCalculator(Iterable<MeasuredData> dataStream, Map<String, List<ExecutorDetails>> comp2Executors,
                               StormTopology rawTopo) {
        this.dataStream = dataStream;
        this.rawTopo = rawTopo;
        comp2Executors.forEach((comp, exeList) -> {
            AggResult[] executorResults;
            if (rawTopo.get_spouts().containsKey(comp)) {
                executorResults = new SpoutAggResult[exeList.size()];
                for (int i = 0; i < executorResults.length; i++) {
                    executorResults[i] = createTaskIndex(new SpoutAggResult(), exeList.get(i));
                }
            } else {
                executorResults = new BoltAggResult[exeList.size()];
                for (int i = 0; i < executorResults.length; i++) {
                    executorResults[i] = createTaskIndex(new BoltAggResult(), exeList.get(i));
                }
            }
            comp2ExecutorResults.put(comp, executorResults);
        });
        firstTasks = comp2Executors.values().stream().flatMap(e -> e.stream()).map(ExecutorDetails::getStartTask)
                .collect(Collectors.toSet());
    }

    //Here, for each executor, we create task2ExecutorResult Entries for each of the tasks, but refer to their corresponding executorResult
    private AggResult createTaskIndex(AggResult executorResult, ExecutorDetails e) {
        IntStream.rangeClosed(e.getStartTask(), e.getEndTask()).forEach((task) -> task2ExecutorResult.put(task, executorResult));
        return executorResult;
    }

    private AggResult parse(MeasuredData measuredData, AggResult dest) {
        // parse send queue and recv queue first
        // Modified version by Tom Fu, for each executor queue, only read once by its first task
        if (firstTasks.contains(measuredData.task)) {
            measuredData.data.computeIfPresent(MetricNames.DURATION, (comp, data) -> {
                dest.addDuration(((Number) data).longValue());
                return data;
            });

            measuredData.data.computeIfPresent(MetricNames.SEND_QUEUE, (comp, data) -> {
                parseQueueResult((Map<String, Number>) data, dest.getSendQueueResult());
                return data;
            });
            measuredData.data.computeIfPresent(MetricNames.RECV_QUEUE, (comp, data) -> {
                parseQueueResult((Map<String, Number>) data, dest.getRecvQueueResult());
                return data;
            });
        }
        if (rawTopo.get_spouts().containsKey(measuredData.component)) {
            Map<String, Object> data = (Map<String, Object>) measuredData.data.get(MetricNames.COMPLETE_LATENCY);
            if (data != null) {
                data.forEach((stream, elementStr) -> {
                    String[] elements = ((String) elementStr).split(",");
                    int cnt = Integer.valueOf(elements[0]);
                    if (cnt > 0) {
                        double val = Double.valueOf(elements[1]);
                        double val_2 = Double.valueOf(elements[2]);
                        ((SpoutAggResult) dest).getCompletedLatency().computeIfAbsent(stream, (k) -> new CntMeanVar())
                                .addAggWin(cnt, val, val_2);
                    }
                });
            }
        } else {
            Map<String, Object> data = (Map<String, Object>) measuredData.data.get(MetricNames.TASK_EXECUTE);
            if (data != null) {
                data.forEach((stream, elementStr) -> {
                    String[] elements = ((String) elementStr).split(",");
                    int cnt = Integer.valueOf(elements[0]);
                    if (cnt > 0) {
                        double val = Double.valueOf(elements[1]);
                        double val_2 = Double.valueOf(elements[2]);
                        ((BoltAggResult) dest).getTupleProcess().computeIfAbsent(stream, (k) -> new CntMeanVar())
                                .addAggWin(cnt, val, val_2);
                    }
                });
            }
        }
        return dest;
    }

    private void parseQueueResult(Map<String, Number> queueMetrics, QueueAggResult queueResult) {
        //Newly updated with Storm version > 10.0, including four incline queue related metrics, refer to package backtype.storm.utils.DisruptorQueue
        double queueArrivalRatePerSecond = queueMetrics.getOrDefault(MetricNames.QUEUE_ARRIVAL_RATE_SECS, Double.valueOf(0)).doubleValue();
        double queueCapacity = queueMetrics.getOrDefault(MetricNames.QUEUE_CAPACITY, Double.valueOf(0)).doubleValue();
        double queueLength = queueMetrics.getOrDefault(MetricNames.QUEUE_POLULATION, Double.valueOf(0)).doubleValue();
        double queueingTimeMilliSecond = queueMetrics.getOrDefault(MetricNames.QUEUE_SOJOURN_TIME_MS, Double.valueOf(0)).doubleValue();

        queueResult.add(queueArrivalRatePerSecond, queueCapacity, queueLength, queueingTimeMilliSecond);
    }

    public void calCMVStat() {
        int count = 0;
        for (MeasuredData measuredData : dataStream) {
            AggResult car = task2ExecutorResult.get(measuredData.task);
            parse(measuredData, car);
            count++;
        }
        LOG.info("calCMVStat, processed measuredData size: " + count);
    }

    public Map<String, AggResult[]> getComp2ExecutorResults() {
        return comp2ExecutorResults;
    }

    public Map<String, AggResult[]> getSpoutResults() {
        return comp2ExecutorResults.entrySet().stream().filter(e -> rawTopo.get_spouts().containsKey(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public Map<String, AggResult[]> getBoltResults() {
        return comp2ExecutorResults.entrySet().stream().filter(e -> rawTopo.get_bolts().containsKey(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
