package resa.metrics;

import org.apache.storm.Config;
import org.apache.storm.hooks.BaseTaskHook;
import org.apache.storm.hooks.info.SpoutAckInfo;
import org.apache.storm.hooks.info.SpoutFailInfo;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.topology.DelegatedSpout;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;
import resa.util.Sampler;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A measurable spout implementation based on system hook
 * <p>
 * Created by ding on 14-4-8.
 */
public class MeasurableSpout extends DelegatedSpout {

    private static final Logger LOG = LoggerFactory.getLogger(MeasurableSpout.class);
    private class MeasurableMsgId {
        final String stream;
        final Object msgId;
        final long startTime;

        private MeasurableMsgId(String stream, Object msgId, long startTime) {
            this.stream = stream;
            this.msgId = msgId;
            this.startTime = startTime;
        }

        public boolean isSampled() {
            return startTime > 0;
        }
    }

    private class SpoutHook extends BaseTaskHook {

        @Override
        public void spoutAck(SpoutAckInfo info) {
            MeasurableMsgId streamMsgId = (MeasurableMsgId) info.messageId;
            if (streamMsgId != null && streamMsgId.isSampled()) {
                long cost = System.currentTimeMillis() - streamMsgId.startTime;
                completeMetric.addMetric(streamMsgId.stream, cost);
                if (cost > qos) {
                    missMetric.addMetric(streamMsgId.stream, cost);
                }
                if (completeStatMetric != null) {
                    completeStatMetric.add(streamMsgId.stream, cost);
                }
            }
        }

        @Override
        public void spoutFail(SpoutFailInfo info) {
            MeasurableMsgId streamMsgId = (MeasurableMsgId) info.messageId;
            if (streamMsgId != null && streamMsgId.isSampled()) {
                if (completeStatMetric != null) {
                    completeStatMetric.fail(streamMsgId.stream);
                }
            }
        }
    }

    private transient CMVMetric completeMetric;
    private Sampler sampler;
    private transient MultiCountMetric emitMetric;
    private transient CMVMetric missMetric;
    private transient CompleteStatMetric completeStatMetric;
    private long lastMetricsSent;
    private long qos;

    public MeasurableSpout(){

    }
    public MeasurableSpout(IRichSpout delegate) {
        super(delegate);
    }

    private long getMetricsDuration() {
        long now = System.currentTimeMillis();
        long duration = now - lastMetricsSent;
        lastMetricsSent = now;
        return duration;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        int interval = Utils.getInt(conf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS));
        completeMetric = context.registerMetric(MetricNames.COMPLETE_LATENCY, new CMVMetric(), interval);
        // register miss metric
        qos = ConfigUtil.getLong(conf, "resa.metric.complete-latency.threshold.ms", Long.MAX_VALUE);
        missMetric = context.registerMetric(MetricNames.MISS_QOS, new CMVMetric(), interval);
        emitMetric = context.registerMetric(MetricNames.EMIT_COUNT, new MultiCountMetric(), interval);
        // register stat metric
        double[] xAxis = Stream.of(((String) conf.getOrDefault("resa.metric.complete-latency.stat.x-axis", ""))
                .split(",")).filter(s -> !s.isEmpty()).mapToDouble(Double::parseDouble).toArray();
        completeStatMetric = xAxis.length > 0 ? context.registerMetric(MetricNames.LATENCY_STAT,
                new CompleteStatMetric(xAxis), interval) : null;
        // register duration metric
        lastMetricsSent = System.currentTimeMillis();
        context.registerMetric(MetricNames.DURATION, this::getMetricsDuration, interval);

        sampler = new Sampler(ConfigUtil.getDouble(conf, ResaConfig.COMP_SAMPLE_RATE, 0.05));
        context.addTaskHook(new SpoutHook());
        super.open(conf, context, new SpoutOutputCollector(collector) {

            @Override
            public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
                return super.emit(streamId, tuple, newStreamMessageId(streamId, messageId));
            }

            @Override
            public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
                super.emitDirect(taskId, streamId, tuple, newStreamMessageId(streamId, messageId));
            }

            private MeasurableMsgId newStreamMessageId(String stream, Object messageId) {
                long startTime;
                if (sampler.shoudSample()) {
                    startTime = System.currentTimeMillis();
                    emitMetric.scope(stream).incr();
                } else {
                    startTime = -1;
                }
                return messageId == null ? null : new MeasurableMsgId(stream, messageId, startTime);
            }
        });

        LOG.info("Preparing MeasurableSpout: " + context.getThisComponentId());
    }

    private Object getUserMsgId(Object msgId) {
        return msgId != null ? ((MeasurableMsgId) msgId).msgId : msgId;
    }

    @Override
    public void ack(Object msgId) {
        super.ack(getUserMsgId(msgId));
    }

    @Override
    public void fail(Object msgId) {
        super.fail(getUserMsgId(msgId));
    }

}
