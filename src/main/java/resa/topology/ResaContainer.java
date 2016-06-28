package resa.topology;

import org.apache.storm.Config;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.drs.ResourceScheduler;
import resa.metrics.FilteredMetricsCollector;
import resa.metrics.MeasuredData;
import resa.metrics.MetricNames;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;
import resa.util.TopologyHelper;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Created by ding on 14-5-5.
 */
public class ResaContainer extends FilteredMetricsCollector {

    //    public static final String REDIS_HOST = "resa.container.metric.redis.host";
//    public static final String REDIS_PORT = "resa.container.metric.redis.port";
//    public static final String REDIS_QUEUE_NAME = "resa.container.metric.redis.queue-name";
    public static final String METRIC_OUTPUT = "resa.container.metric.output";

    private static final Logger LOG = LoggerFactory.getLogger(ResaContainer.class);

    private ResourceScheduler resourceScheduler = new ResourceScheduler();
    private ContainerContext ctx;
    private Nimbus.Client nimbus;
    private String topologyName;
    private String topologyId;
    private Map<String, Object> conf;
//    private Thread metricSendThread;
//    private final BlockingQueue<String> metricsQueue = new LinkedBlockingDeque<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private boolean outputMetrics;

    @Override
    public void prepare(Map conf, Object arg, TopologyContext context, IErrorReporter errorReporter) {
        super.prepare(conf, arg, context, errorReporter);
        this.conf = conf;
        this.topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
        // connected to nimbus
        nimbus = NimbusClient.getConfiguredClient(conf).getClient();
        topologyId = TopologyHelper.getTopologyId(nimbus, topologyName);
        // add approved metric name
        addApprovedMetirc("__sendqueue", MetricNames.SEND_QUEUE);
        addApprovedMetirc("__receive", MetricNames.RECV_QUEUE);
        addApprovedMetirc(MetricNames.COMPLETE_LATENCY);
        addApprovedMetirc(MetricNames.TASK_EXECUTE);
        addApprovedMetirc(MetricNames.EMIT_COUNT);
        addApprovedMetirc(MetricNames.DURATION);

        ctx = new ContainerContextImpl(context.getRawTopology(), conf);
        // topology optimizer will start its own thread
        // if more services required to start, maybe we need to extract a new interface here
        resourceScheduler.init(ctx);
        resourceScheduler.start();

        outputMetrics = (Boolean) conf.getOrDefault(METRIC_OUTPUT, Boolean.FALSE);
        outputTopologyInfo(context);
//        if (outputMetrics) {
//            addMetricsOutputListener();
//            metricSendThread = createMetricsSendThread();
//            metricSendThread.start();
//            LOG.info("Metrics send thread started");
//        }
    }

    private void outputTopologyInfo(TopologyContext context) {
        Map<String, Object> topo = new LinkedHashMap<>();
        topo.put("id", context.getStormId());
        topo.put("spouts", new ArrayList<>(context.getRawTopology().get_spouts().keySet()));
        topo.put("bolts", new ArrayList<>(context.getRawTopology().get_bolts().keySet()));
        topo.put("targets", new HashMap<>());
        for (String comp : context.getComponentIds()) {
            Map<String, List<String>> targets = new HashMap<>();
            context.getTargets(comp).forEach((stream, g) -> {
                targets.put(stream, new ArrayList<>(g.keySet()));
            });
            ((Map<String, Object>) topo.get("targets")).put(comp, targets);
        }
        ctx.emitMetric("topology.info", topo);
    }

//    private void addMetricsOutputListener() {
//        ctx.addListener(measuredData -> {
//            String key = "task." + measuredData.component + "." + measuredData.task;
//            ctx.emitMetric(key, measuredData.data);
//        });
//    }

    private String object2Json(Object o) {
        try {
            return objectMapper.writeValueAsString(o);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

//    private Thread createMetricsSendThread() {
//        String jedisHost = (String) conf.get(REDIS_HOST);
//        int jedisPort = ((Number) conf.get(REDIS_PORT)).intValue();
//        String queueName = (String) conf.get(REDIS_QUEUE_NAME);
//        // queue name is not exist, use topology id as default
//        if (queueName == null || queueName.isEmpty()) {
//            queueName = topologyId + "-container";
//        }
//        final String finalQueueName = queueName;
//        Thread t = new Thread("Metrics send thread") {
//            private Jedis jedis;
//
//            @Override
//            public void run() {
//                String value = null;
//                while (true) {
//                    if (value == null) {
//                        try {
//                            value = metricsQueue.take();
//                        } catch (InterruptedException e) {
//                            break;
//                        }
//                    }
//                    try {
//                        getJedisInstance(jedisHost, jedisPort).rpush(finalQueueName, value);
//                        value = null;
//                    } catch (Exception e) {
//                        closeJedis();
//                        Utils.sleep(1);
//                    }
//                }
//                closeJedis();
//                LOG.info("Metrics send thread exit");
//            }
//
//            /* get a jedis instance, create a one if necessary */
//            private Jedis getJedisInstance(String jedisHost, int jedisPort) {
//                if (jedis == null) {
//                    jedis = new Jedis(jedisHost, jedisPort);
//                    LOG.info("connecting to redis server {} on port {}", jedisHost, jedisPort);
//                }
//                return jedis;
//            }
//
//            private void closeJedis() {
//                if (jedis != null) {
//                    try {
//                        LOG.info("disconnecting redis server " + jedisHost);
//                        jedis.disconnect();
//                    } catch (Exception e) {
//                    }
//                    jedis = null;
//                }
//            }
//        };
//        t.setDaemon(true);
//        return t;
//    }

    private class ContainerContextImpl extends ContainerContext {

        protected ContainerContextImpl(StormTopology topology, Map<String, Object> conf) {
            super(topology, conf);
        }

        @Override
        public void emitMetric(String name, Object data) {
            if (outputMetrics) {
                String val = name + "->" + object2Json(data);
                LOG.info(val);
//                metricsQueue.add(val);
            }
        }

        @Override
        public Map<String, List<ExecutorDetails>> runningExecutors() {
            return TopologyHelper.getTopologyExecutors(nimbus, topologyId).entrySet().stream()
                    .filter(e -> !Utils.isSystemId(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        @Override
        public boolean requestRebalance(Map<String, Integer> allocation, int numWorkers) {
            RebalanceOptions options = new RebalanceOptions();
            //set rebalance options
            options.set_num_workers(numWorkers);
            options.set_num_executors(allocation);
            int waitingSecs = ConfigUtil.getInt(conf, ResaConfig.REBALANCE_WAITING_SECS, -1);
            if (waitingSecs >= 0) {
                options.set_wait_secs(waitingSecs);
            }
            try {
                nimbus.rebalance(topologyName, options);
                LOG.info("do rebalance successfully for topology " + topologyName);
                return true;
            } catch (Exception e) {
                LOG.warn("do rebalance failed for topology " + topologyName, e);
            }
            return false;
        }
    }

    @Override
    protected void handleSelectedDataPoints(IMetricsConsumer.TaskInfo taskInfo,
                                            Collection<IMetricsConsumer.DataPoint> dataPoints) {
        Map<String, Object> ret = dataPoints.stream().collect(Collectors.toMap(p -> p.name, p -> p.value));
        MeasuredData measuredData = new MeasuredData(taskInfo.srcComponentId, taskInfo.srcTaskId,
                taskInfo.timestamp, ret);
        ctx.getListeners().forEach(l -> l.measuredDataReceived(measuredData));
    }

    @Override
    public void cleanup() {
        super.cleanup();
        resourceScheduler.stop();
//        metricsQueue.clear();
//        metricSendThread.interrupt();
    }
}
