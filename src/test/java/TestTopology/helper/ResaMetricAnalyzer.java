package TestTopology.helper;

import org.apache.storm.Config;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import resa.optimize.AggResultCalculator;
import resa.optimize.AllocCalculator;
import resa.optimize.MMKAllocCalculator;
import resa.util.ResaConfig;
import resa.util.TopologyHelper;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by Tom.fu on 5/5/2014. Just for testing purpose
 */
public class ResaMetricAnalyzer {

    //There are three types of output metric information.
    // "drs.alloc->{\"status\":\"FEASIBLE\",\"minReqOptAllocation\":{\"2Path-BoltA-NotP\":1,\"2Path-BoltA-P\":1,\"2Path-BoltB\":1,\"2Path-Spout\":1},
    //"topology.info"->
    //"task.2Path-Spout.16->{\    private Map<String, Object> conf = ResaConfig.create(true);

    private Map<String, Object> conf = ResaConfig.create(true);
    public static void main(String[] args) {
        System.out.println("ResaMetricAnalyzer based on ResaDataSource");
        try {
            String nimbusHost = args[0];
            String topName = args[1];
            String redisHost = args[2];
            int redisPort = Integer.parseInt(args[3]);
            String metricQueue = args[4];
            long sleepTime = Long.parseLong(args[5]);
            int maxAllowedExecutors = Integer.parseInt(args[6]);
            double qos = Double.parseDouble(args[7]);
            int historySize = Integer.parseInt(args[8]);
            int ignoreSize = Integer.parseInt(args[9]);
            double sampleRate = Double.parseDouble(args[10]);
            System.out.println("NimbusHost: " + nimbusHost + "Topology name: " + topName +
                    ", RedisHost+Port: " + redisHost + ":" + redisPort + ", metricQueue: " + metricQueue +
                    ", sleepTime: " + sleepTime + ", maxAllowed: " + maxAllowedExecutors + ", qos (ms): " + qos + ", sampleRate: " + sampleRate);
            ResaMetricAnalyzer rt = new ResaMetricAnalyzer();
            rt.testMakeUsingTopologyHelperForkTopology(nimbusHost, topName, redisHost, redisPort, metricQueue,
                    sleepTime, maxAllowedExecutors, qos, historySize, ignoreSize, sampleRate);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void testMakeUsingTopologyHelperForkTopology(
            String nimbusHost, String topoName, String redisHost, int redisPort, String metricQueue,
            long sleepTime, int allewedExecutorNum, double qos, int historySize, int ignoreSize, double sampleRate) throws Exception {

        conf.put(Config.NIMBUS_HOST, nimbusHost);
        conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
        conf.put(Config.TOPOLOGY_DEBUG, true);

        conf.put("resa.opt.smd.qos.ms", qos);
        conf.put("resa.opt.win.history.size", historySize);
        conf.put("resa.opt.win.history.size.ignore", ignoreSize);
        conf.put("resa.comp.sample.rate", sampleRate);

        conf.put(ResaConfig.ALLOWED_EXECUTOR_NUM, allewedExecutorNum);

        String host = redisHost;
        int port = redisPort;
        int maxLen = 5000;

        NimbusClient nimbusClient = NimbusClient.getConfiguredClient(conf);
        Nimbus.Client nimbus = nimbusClient.getClient();

        String topoId = TopologyHelper.getTopologyId(nimbus, topoName);
        TopologyInfo topoInfo = nimbus.getTopologyInfo(topoId);

        long startTime = System.currentTimeMillis();

        Map<String, Integer> currAllocation = topoInfo.get_executors().stream().filter(e -> !Utils.isSystemId(e.get_component_id()))
                .collect(Collectors.groupingBy(e -> e.get_component_id(),
                        Collectors.reducing(0, e -> 1, (i1, i2) -> i1 + i2)));

        AllocCalculator smdm = new MMKAllocCalculator();

        smdm.init(conf, currAllocation, nimbus.getUserTopology(topoId));

        while (true) {
            Utils.sleep(sleepTime);

            topoInfo = nimbus.getTopologyInfo(topoId);
            Map<String, Integer> updatedAllocation = topoInfo.get_executors().stream().filter(e -> !Utils.isSystemId(e.get_component_id()))
                    .collect(Collectors.groupingBy(e -> e.get_component_id(),
                            Collectors.reducing(0, e -> 1, (i1, i2) -> i1 + i2)));

            Map<String, List<ExecutorDetails>> comp2Executors = TopologyHelper.getTopologyExecutors(topoName, conf)
                    .entrySet().stream().filter(e -> !Utils.isSystemId(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            AggResultCalculator resultCalculator = new AggResultCalculator(
                    ResaDataSource.readData(host, port, metricQueue, maxLen), comp2Executors, nimbus.getUserTopology(topoId));
            resultCalculator.calCMVStat();

            long currTime = System.currentTimeMillis();
            System.out.println("------Report on: " + currTime + ",last for: " + (currTime - startTime)/60000 + " minutes, " + (currTime - startTime) + " secs.----------");
            if (currAllocation.equals(updatedAllocation)) {
                System.out.println(currAllocation + "-->" + smdm.calc(resultCalculator.getComp2ExecutorResults(), allewedExecutorNum));
            } else {
                currAllocation = updatedAllocation;
                smdm.allocationChanged(currAllocation);
                ResaDataSource.clearQueue(host, port, metricQueue);
                System.out.println("Allocation updated to " + currAllocation);
            }
        }
    }
}
