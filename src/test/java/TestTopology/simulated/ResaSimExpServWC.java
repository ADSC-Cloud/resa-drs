package TestTopology.simulated;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import resa.metrics.RedisMetricsCollector;
import resa.topology.ResaTopologyBuilder;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;

import java.io.File;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class ResaSimExpServWC {

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.out.println("Enter path to config file!");
            System.exit(0);
        }
        Config conf = ConfigUtil.readConfig(new File(args[0]));

        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[0]);
        }

        TopologyBuilder builder = new ResaTopologyBuilder();
        int defaultTaskNum = ConfigUtil.getInt(conf, "defaultTaskNum", 10);

        String host = (String)conf.get("redis.host");
        int port = ConfigUtil.getInt(conf, "redis.port", 6379);
        String queueName = (String)conf.get("redis.sourceQueueName");

        builder.setSpout("chain-Spout", new TASentenceSpout(host, port, queueName),
                ConfigUtil.getInt(conf, "chain-spout.parallelism", 1));

        double chainBoltA_mu = ConfigUtil.getDouble(conf, "chain-BoltA.mu", 1.0);
        builder.setBolt("chain-BoltA", new TASplitSentence(() -> (long) (-Math.log(Math.random()) * 1000.0 / chainBoltA_mu)),
                ConfigUtil.getInt(conf, "chain-BoltA.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .shuffleGrouping("chain-Spout");

        double chainBoltB_mu = ConfigUtil.getDouble(conf, "chain-BoltB.mu", 1.0);
        builder.setBolt("chain-BoltB", new TAWordCounter(() -> (long) (-Math.log(Math.random()) * 1000.0 / chainBoltB_mu)),
                ConfigUtil.getInt(conf, "chain-BoltB.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .shuffleGrouping("chain-BoltA");

        conf.setNumWorkers(ConfigUtil.getInt(conf, "chain-NumOfWorkers", 1));
        conf.setMaxSpoutPending(ConfigUtil.getInt(conf, "chain-MaxSpoutPending", 0));
        conf.setDebug(ConfigUtil.getBoolean(conf, "DebugTopology", false));
        conf.setStatsSampleRate(ConfigUtil.getDouble(conf, "StatsSampleRate", 1.0));

        if (ConfigUtil.getBoolean(conf, "EnableRedisMetricsCollector", false)) {
            conf.registerMetricsConsumer(RedisMetricsCollector.class);
            System.out.println("RedisMetricsCollector is registered");
        }

        ResaConfig resaConfig = ResaConfig.create();
        resaConfig.putAll(conf);
        if (ConfigUtil.getBoolean(conf, "EnableResaMetricsCollector", false)) {
            resaConfig.addDrsSupport();
            resaConfig.put(ResaConfig.REBALANCE_WAITING_SECS, 0);
            System.out.println("ResaMetricsCollector is registered");
        }

        StormSubmitter.submitTopology("resa-Sim-chain-top", resaConfig, builder.createTopology());
    }
}
