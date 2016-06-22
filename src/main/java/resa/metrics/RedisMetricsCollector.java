package resa.metrics;

import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Write metrics to redis server.
 * <p>
 * Created by ding on 13-12-11.
 */
public class RedisMetricsCollector extends FilteredMetricsCollector {

    public static class QueueElement {
        public final String queueName;
        public final String data;

        public QueueElement(String queueName, String data) {
            this.queueName = queueName;
            this.data = data;
        }
    }

    public static final String REDIS_HOST = "resa.metric.redis.host";
    public static final String REDIS_PORT = "resa.metric.redis.port";
    public static final String REDIS_QUEUE_NAME = "resa.metric.redis.queue-name";

    private static final Logger LOG = LoggerFactory.getLogger(RedisMetricsCollector.class);

    private transient Jedis jedis;
    private String jedisHost;
    private int jedisPort;
    private String queueName;

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context,
                        IErrorReporter errorReporter) {
        super.prepare(stormConf, registrationArgument, context, errorReporter);
        jedisHost = (String) stormConf.get(REDIS_HOST);
        jedisPort = ((Number) stormConf.get(REDIS_PORT)).intValue();
        queueName = (String) stormConf.get(REDIS_QUEUE_NAME);
        // queue name is not exist, use topology id as default
        if (queueName == null) {
            queueName = context.getStormId() + "-metrics";
        }
        LOG.info("Write metrics to redis server " + jedisHost + ":" + jedisPort);
    }

    /* get a jedis instance, create a one if necessary */
    private Jedis getJedisInstance() {
        if (jedis == null) {
            jedis = new Jedis(jedisHost, jedisPort);
            LOG.info("connecting to redis server " + jedisHost);
        }
        return jedis;
    }

    private void closeJedis() {
        if (jedis != null) {
            try {
                LOG.info("disconnecting redis server " + jedisHost);
                jedis.disconnect();
            } catch (Exception e) {
            }
            jedis = null;
        }
    }

    @Override
    protected void handleSelectedDataPoints(TaskInfo taskInfo,
                                            Collection<DataPoint> dataPoints) {
        List<QueueElement> data = dataPoints2QueueElement(taskInfo, dataPoints);
        if (data == null) {
            return;
        }
        // LOG.debug("data size is " + data.size());
        // push to redis
        for (QueueElement e : data) {
            try {
                getJedisInstance().rpush(e.queueName, e.data);
            } catch (Exception e1) {
                LOG.info("push data to redis failed", e1);
                closeJedis();
            }
        }
    }

    protected List<QueueElement> dataPoints2QueueElement(TaskInfo taskInfo,
                                                         Collection<DataPoint> dataPoints) {
        Map<String, Object> ret = dataPoints.stream().collect(Collectors.toMap(p -> p.name, p -> p.value));
        //data format is "srcComponentId:taskId:timestamp->data point json"
        String data = taskInfo.srcComponentId + ':' + taskInfo.srcTaskId + ':' + taskInfo.timestamp
                + "->" + object2Json(ret);
        return Arrays.asList(createDefaultQueueElement(data));
    }

    private String object2Json(Object o) {
        try {
            return objectMapper.writeValueAsString(o);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected QueueElement createDefaultQueueElement(String data) {
        return new QueueElement(queueName, data);
    }


    @Override
    public void cleanup() {
        closeJedis();
    }
}