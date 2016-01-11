package TestTopology.helper;

import backtype.storm.Config;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import resa.metrics.RedisMetricsCollector;
import resa.optimize.AggResult;
import resa.optimize.AggResultCalculator;
import resa.optimize.AllocResult;
import resa.optimize.SimpleGeneralAllocCalculator;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;
import resa.util.TopologyHelper;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Created by Tom.fu on 5/5/2014.
 */
public class RedisMetricAnalyzer {

     //redis-cli lrange tomVLDTopEchoExpFInBC-s1-1024-768-L1-p25-102-1430416361-container 0 -1 | grep drs | awk '{split($0,a,"->"); print a[2]}' > tmp.log

    public static void main(String[] args) {
        System.out.println("this is a test!!");
        try {
            String topName = args[0];

            Config conf = ConfigUtil.readConfig(new File(args[1]));
            //by default this shall be "metrics"
            String metricQueue = (String)conf.get(RedisMetricsCollector.REDIS_QUEUE_NAME);
            long sleepTime = ConfigUtil.getLong(conf, ResaConfig.OPTIMIZE_INTERVAL, 30l);
            int maxAllowedExecutors = ConfigUtil.getInt(conf, ResaConfig.ALLOWED_EXECUTOR_NUM, 25);
            double qos = ConfigUtil.getDouble(conf, ResaConfig.OPTIMIZE_SMD_QOS_MS, 5000.0);

            System.out.println("Topology name: " + topName + ", metricQueue: " + metricQueue
                    + ", sleepTime: " + sleepTime + ", maxAllowed: " + maxAllowedExecutors + ", qos: " + qos);
            RedisMetricAnalyzer rt = new RedisMetricAnalyzer();
            rt.testMakeUsingTopologyHelperForkTopology(topName, metricQueue, sleepTime, maxAllowedExecutors, qos, conf);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void testMakeUsingTopologyHelperForkTopology(String topoName, String metricQueue,
                                                        long sleepTime, int allewedExecutorNum, double qos, Map conf) throws Exception {

        conf.put(Config.NIMBUS_HOST, "192.168.0.31");
        conf.put(Config.NIMBUS_THRIFT_PORT, 6627);

        String host = (String)conf.get("redis.host");
        int port = ConfigUtil.getInt(conf, "redis.port", 6379);
        int maxLen = 5000;

        NimbusClient nimbusClient = NimbusClient.getConfiguredClient(conf);
        Nimbus.Client nimbus = nimbusClient.getClient();

        String topoId = TopologyHelper.getTopologyId(nimbus, topoName);
        TopologyInfo topoInfo = nimbus.getTopologyInfo(topoId);

        Map<String, Integer> currAllocation = topoInfo.get_executors().stream().filter(e -> !Utils.isSystemId(e.get_component_id()))
                .collect(Collectors.groupingBy(e -> e.get_component_id(),
                        Collectors.reducing(0, e -> 1, (i1, i2) -> i1 + i2)));

        SimpleGeneralAllocCalculator smdm = new SimpleGeneralAllocCalculator();
        smdm.init(conf, currAllocation, nimbus.getUserTopology(topoId));

        for (int i = 0; i < 10000; i++) {
            Utils.sleep(sleepTime);

            topoInfo = nimbus.getTopologyInfo(topoId);

            Map<String, Integer> updatedAllocation = topoInfo.get_executors().stream().filter(e -> !Utils.isSystemId(e.get_component_id()))
                    .collect(Collectors.groupingBy(e -> e.get_component_id(),
                            Collectors.reducing(0, e -> 1, (i1, i2) -> i1 + i2)));

            Map<String, List<ExecutorDetails>> comp2Executors = TopologyHelper.getTopologyExecutors(topoName, conf)
                    .entrySet().stream().filter(e -> !Utils.isSystemId(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            AggResultCalculator resultCalculator = new AggResultCalculator(
                    RedisDataSource.readData(host, port, metricQueue, maxLen), comp2Executors, nimbus.getUserTopology(topoId));
            resultCalculator.calCMVStat();

            System.out.println("-------------Report on: " + System.currentTimeMillis() + "------------------------------");
            Objects.requireNonNull(currAllocation);
            Objects.requireNonNull(updatedAllocation);
            if (currAllocation.equals(updatedAllocation)) {
                Map<String, AggResult[]> tmp = resultCalculator.getComp2ExecutorResults();
                Objects.requireNonNull(tmp);
                AllocResult ret = smdm.calc(tmp, allewedExecutorNum);
                System.out.println(currAllocation + "-->" + (Objects.isNull(ret) ? "null" : ret));
            } else {
                currAllocation = updatedAllocation;
                smdm.allocationChanged(currAllocation);
                RedisDataSource.clearQueue(host, port, metricQueue);
                System.out.println("Allocation updated to " + currAllocation);
            }
        }
    }
}
