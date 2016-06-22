package resa.util;

import org.apache.storm.generated.*;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.ThriftTopologyUtils;
import org.apache.storm.utils.Utils;
import org.apache.storm.thrift.TException;
import org.json.simple.JSONValue;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Helper class to get access online topology running details.
 *
 * @author Troy Ding
 */
public class TopologyHelper {

    /**
     * Get all running topology's details.
     *
     * @param conf storm's conf, used to connect nimbus
     * @return null if nimbus is not available, otherwise all TopologyDetails
     */
    public static Topologies getTopologyDetails(Map<String, Object> conf) {
        NimbusClient nimbusClient = NimbusClient.getConfiguredClient(conf);
        try {
            Nimbus.Client nimbus = nimbusClient.getClient();
            Map<String, TopologyDetails> topologies = nimbus.getClusterInfo().get_topologies().stream()
                    .map(topoSummary -> getTopologyDetails(nimbus, topoSummary))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toMap(topoDetails -> topoDetails.getId(), topoDetails -> topoDetails));
            return new Topologies(topologies);
        } catch (TException e) {
        } finally {
            nimbusClient.close();
        }
        return null;
    }

    public static Map<Integer, String> getTask2Host(Map<String, Object> conf, String topoId) {
        NimbusClient nimbusClient = NimbusClient.getConfiguredClient(conf);
        try {
            Nimbus.Client nimbus = nimbusClient.getClient();
            TopologyInfo topoInfo = nimbus.getTopologyInfo(topoId);
            Map<Integer, String> task2Host = new HashMap<>();
            topoInfo.get_executors().forEach(e -> {
                ExecutorInfo eInfo = e.get_executor_info();
                for (int i = eInfo.get_task_start(); i <= eInfo.get_task_end(); i++) {
                    task2Host.put(i, e.get_host());
                }
            });
            return task2Host;
        } catch (Exception e) {
        } finally {
            nimbusClient.close();
        }
        return null;
    }

    public static Map<String, Map<ExecutorDetails, String>> getAssignment(Map<String, Object> conf, String topoId) {
        NimbusClient nimbusClient = NimbusClient.getConfiguredClient(conf);
        try {
            Nimbus.Client nimbus = nimbusClient.getClient();
            TopologyInfo topoInfo = nimbus.getTopologyInfo(topoId);
            Map<String, Map<ExecutorDetails, String>> assignment = new HashMap<>();
            topoInfo.get_executors().forEach(e -> {
                ExecutorInfo eInfo = e.get_executor_info();
                assignment.computeIfAbsent(e.get_component_id(), (k) -> new HashMap<>())
                        .put(new ExecutorDetails(eInfo.get_task_start(), eInfo.get_task_end()),
                                e.get_host() + ":" + e.get_port());
            });
            return assignment;
        } catch (Exception e) {
        } finally {
            nimbusClient.close();
        }
        return null;
    }

    /**
     * Kill a specified topology.
     *
     * @param topoName
     * @param conf
     */
    public static void killTopology(String topoName, Map<String, Object> conf) {
        NimbusClient nimbusClient = NimbusClient.getConfiguredClient(conf);
        try {
            Nimbus.Client nimbus = nimbusClient.getClient();
            nimbus.killTopology(topoName);
        } catch (NotAliveException e){
        } catch( TException e) {
        } finally {
            nimbusClient.close();
        }
    }

    public static void killTopology(String topoName, int waitSecs, Map<String, Object> conf) {
        NimbusClient nimbusClient = NimbusClient.getConfiguredClient(conf);
        try {
            Nimbus.Client nimbus = nimbusClient.getClient();
            KillOptions killOptions = new KillOptions();
            killOptions.set_wait_secs(waitSecs);
            nimbus.killTopologyWithOpts(topoName, killOptions);
        } catch (NotAliveException e){
        } catch( TException e) {
        } finally {
            nimbusClient.close();
        }
    }

    private static TopologyDetails getTopologyDetails(Nimbus.Client nimbus, TopologySummary topologySummary) {
        String topoId = topologySummary.get_id();
        int numWorkers = topologySummary.get_num_workers();
        StormTopology topology;
        TopologyInfo topologyInfo;
        Map topologyConf;
        try {
            topology = nimbus.getUserTopology(topoId);
            topologyInfo = nimbus.getTopologyInfo(topoId);
            topologyConf = (Map) JSONValue.parse(nimbus.getTopologyConf(topoId));
        } catch (NotAliveException e){
            return null;
        } catch( TException e) {
            return null;
        }
        Map<ExecutorDetails, String> exe2Components = topologyInfo.get_executors().stream()
                .collect(Collectors.toMap(e -> toExecutorDetails(e.get_executor_info()), e -> e.get_component_id()));
        return new TopologyDetails(topologyInfo.get_id(), topologyConf, topology, numWorkers, exe2Components);
    }

    public static Map<String, Integer> getComponentExecutorCount(TopologyDetails topoDetails) {
        return topoDetails.getExecutorToComponent().entrySet().stream()
                .collect(Collectors.groupingBy(Map.Entry::getValue, Collectors.reducing(0, e -> 1, (i, j) -> i + j)));
    }


    /**
     * Get a running topology's details.
     *
     * @param topoName topology's name
     * @param conf     storm's conf, used to connect nimbus
     * @return null if topology is not exist, otherwise a TopologyDetails instance
     */
    public static TopologyDetails getTopologyDetails(String topoName, Map<String, Object> conf) {
        NimbusClient nimbusClient = NimbusClient.getConfiguredClient(conf);
        try {
            Nimbus.Client nimbus = nimbusClient.getClient();
            ClusterSummary cluster = nimbus.getClusterInfo();
            Optional<TopologySummary> topo = cluster.get_topologies().stream()
                    .filter(e -> e.get_name().equals(topoName)).findFirst();
            return topo.map(topoSummary -> topoSummary == null ? null : getTopologyDetails(nimbus, topoSummary)).get();
        } catch (TException e) {
        } finally {
            nimbusClient.close();
        }
        return null;
    }

    /**
     * Get component tasks from TopologyDetails object
     *
     * @param topoDetails
     * @param ignoreSystemComp whether system component should be ignore
     * @return
     */
    public static Map<String, List<Integer>> componentToTasks(TopologyDetails topoDetails, boolean ignoreSystemComp) {
        Predicate<Map.Entry<ExecutorDetails, String>> p = null;
        if (ignoreSystemComp) {
            p = e -> !Utils.isSystemId(e.getValue());
        }
        return componentToTasks(topoDetails, p);
    }

//    /**
//     * Get bolt component tasks from TopologyDetails object
//     *
//     * @param topoDetails
//     * @return
//     */
//    public static Map<String, List<Integer>> boltComponentToTasks(TopologyDetails topoDetails) {
//        Set<String> bolts = topoDetails.getTopology().get_bolts().keySet();
//        Predicate<Map.Entry<ExecutorDetails, String>> p = e -> bolts.contains(e.getValue());
//        return componentToTasks(topoDetails, p);
//    }
//
//    /**
//     * Get spout component tasks from TopologyDetails object
//     *
//     * @param topoDetails
//     * @return
//     */
//    public static Map<String, List<Integer>> spoutComponentToTasks(TopologyDetails topoDetails) {
//        Set<String> spouts = topoDetails.getTopology().get_spouts().keySet();
//        Predicate<Map.Entry<ExecutorDetails, String>> p = e -> spouts.contains(e.getValue());
//        return componentToTasks(topoDetails, p);
//    }

    /**
     * Get component tasks from TopologyDetails object
     *
     * @param topoDetails
     * @return
     */
    private static Map<String, List<Integer>> componentToTasks(TopologyDetails topoDetails,
                                                               Predicate<Map.Entry<ExecutorDetails, String>> p) {
        Stream<Map.Entry<ExecutorDetails, String>> stream = topoDetails.getExecutorToComponent().entrySet().stream();
        if (p != null) {
            stream = stream.filter(p);
        }
        return stream.collect(Collectors.groupingBy(Map.Entry::getValue,
                        Collectors.mapping(e -> getTaskIds(e.getKey()),
                                Collector.of(ArrayList::new, (all, l) -> all.addAll(l),
                                        (all, l) -> {
                                            all.addAll(l);
                                            return all;
                                        }
                                )
                        )
                )
        );
    }

    /**
     * Get component tasks from TopologyDetails object
     *
     * @param topoDetails
     * @return
     */
    public static Map<String, List<Integer>> componentToTasks(TopologyDetails topoDetails) {
        return componentToTasks(topoDetails, false);
    }

    private static ExecutorDetails toExecutorDetails(ExecutorInfo executorInfo) {
        return new ExecutorDetails(executorInfo.get_task_start(), executorInfo.get_task_end());
    }

    private static List<Integer> getTaskIds(ExecutorInfo executorInfo) {
        int start = executorInfo.get_task_start();
        int end = executorInfo.get_task_end();
        return start == end ? Arrays.asList(start)
                : IntStream.rangeClosed(start, end).boxed().collect(Collectors.toList());
    }

    public static List<Integer> getTaskIds(ExecutorDetails executorDetails) {
        int start = executorDetails.getStartTask();
        int end = executorDetails.getEndTask();
        return start == end ? Arrays.asList(start)
                : IntStream.rangeClosed(start, end).boxed().collect(Collectors.toList());
    }

    public static String topologyId2Name(String topologyId) {
        int split = topologyId.indexOf('-');
        return topologyId.substring(0, split);
    }

    public static Map<String, List<ExecutorDetails>> getTopologyExecutors(Nimbus.Client nimbus, String topoId) {
        try {
            TopologyInfo topoInfo = nimbus.getTopologyInfo(topoId);
            return parseCompExecutors(topoInfo);
        } catch (Exception e) {
        }
        return null;
    }

    public static Map<String, List<ExecutorDetails>> getTopologyExecutors(String topoName, Map<String, Object> conf) {
        NimbusClient nimbusClient = NimbusClient.getConfiguredClient(conf);
        try {
            Nimbus.Client nimbus = nimbusClient.getClient();
            String topoId = getTopologyId(nimbus, topoName);
            return getTopologyExecutors(nimbus, topoId);
        } catch (Exception e) {
        } finally {
            nimbusClient.close();
        }
        return null;
    }

    public static Map<String, List<ExecutorDetails>> parseCompExecutors(TopologyInfo topoInfo) {
        return topoInfo.get_executors().stream().collect(Collectors.groupingBy(ExecutorSummary::get_component_id,
                Collectors.mapping(e -> toExecutorDetails(e.get_executor_info()), Collectors.toList())));
    }

    public static String getTopologyId(String topoName, Map<String, Object> conf) {
        NimbusClient nimbusClient = NimbusClient.getConfiguredClient(conf);
        try {
            Nimbus.Client nimbus = nimbusClient.getClient();
            return getTopologyId(nimbus, topoName);
        } finally {
            nimbusClient.close();
        }
    }

    public static String getTopologyId(Nimbus.Client nimbus, String topoName) {
        try {
            ClusterSummary cluster = nimbus.getClusterInfo();
            return cluster.get_topologies().stream()
                    .filter(e -> e.get_name().equals(topoName)).findFirst()
                    .map(topoSummary -> topoSummary == null ? null : topoSummary.get_id()).get();
        } catch (TException e) {
        }
        return null;
    }

    public static GeneralTopologyContext getGeneralTopologyContext(String topoName, Map<String, Object> conf) {
        NimbusClient nimbusClient = NimbusClient.getConfiguredClient(conf);
        try {
            Nimbus.Client nimbus = nimbusClient.getClient();
            return getGeneralTopologyContext(nimbus, topoName);
        } finally {
            nimbusClient.close();
        }
    }

    private static Map<String, Fields> getStream2fields(String comp, StormTopology topo) {
        return ThriftTopologyUtils.getComponentCommon(topo, comp).get_streams().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new Fields(e.getValue().get_output_fields())));
    }

    public static GeneralTopologyContext getGeneralTopologyContext(Nimbus.Client nimbus, String topoName) {
        try {
            ClusterSummary cluster = nimbus.getClusterInfo();
            Optional<TopologySummary> topo = cluster.get_topologies().stream()
                    .filter(e -> e.get_name().equals(topoName)).findFirst();
            if (!topo.isPresent()) {
                throw new IllegalArgumentException("Topology is not exist: " + topoName);
            }
            String topoId = topo.get().get_id();
            TopologyInfo topologyInfo = nimbus.getTopologyInfo(topoId);
            Map topologyConf = (Map) JSONValue.parse(nimbus.getTopologyConf(topoId));
            Map<String, List<Integer>> comp2Tasks = new HashMap<>();
            topologyInfo.get_executors().forEach(e -> comp2Tasks.computeIfAbsent(e.get_component_id(),
                    (k) -> new ArrayList<Integer>()).addAll(getTaskIds(e.get_executor_info())));
            Map<Integer, String> task2Comp = new HashMap<>();
            comp2Tasks.forEach((comp, tasks) -> tasks.forEach((t) -> task2Comp.put(t, comp)));
            StormTopology sysTopology = nimbus.getTopology(topoId);
            Map<String, Map<String, Fields>> component2Stream2Fields = ThriftTopologyUtils.getComponentIds(sysTopology)
                    .stream().collect(Collectors.toMap(comp -> comp, comp -> getStream2fields(comp, sysTopology)));
            return new GeneralTopologyContext(nimbus.getUserTopology(topoId), topologyConf, task2Comp, comp2Tasks,
                    component2Stream2Fields, topoId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
