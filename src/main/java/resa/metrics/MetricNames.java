package resa.metrics;

/**
 * Define all the metrics collected by resa
 * <p>
 * Created by ding on 14-4-26.
 */
public class MetricNames {

    public static final String COMPLETE_LATENCY = "complete-latency";
    public static final String MISS_QOS = "miss-qos";
    public static final String LATENCY_STAT = "latency-stat";
    public static final String TASK_EXECUTE = "execute";
    public static final String DURATION = "duration";
    public static final String SEND_QUEUE = "send-queue";
    public static final String RECV_QUEUE = "recv-queue";
    public static final String EMIT_COUNT = "emit";
    public static final String SERIALIZED_SIZE = "serialized";

    //newly added for queue related metrics, with Storm version after 10.0
    public static final String QUEUE_ARRIVAL_RATE_SECS = "arrival_rate_secs";
    public static final String QUEUE_CAPACITY = "capacity";
    public static final String QUEUE_POLULATION = "population";
    public static final String QUEUE_SOJOURN_TIME_MS = "sojourn_time_ms";
}
