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
public class ResaSimExpServWC2Path {


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

        builder.setSpout("2Path-Spout", new TASentenceSpout2Path(host, port, queueName,
                        ConfigUtil.getDouble(conf, "2Path-spout.prob", 1.0)),
                ConfigUtil.getInt(conf, "2Path-spout.parallelism", 1));

        double boltAP_mu = ConfigUtil.getDouble(conf, "2Path-BoltA-P.mu", 1.0);
        double boltAnotP_mu = ConfigUtil.getDouble(conf, "2Path-BoltA-NotP.mu", 1.0);

        builder.setBolt("2Path-BoltA-P",
                new TASplitSentence(() -> (long) (-Math.log(Math.random()) * 1000.0 / boltAP_mu)),
                ConfigUtil.getInt(conf, "2Path-BoltA-P.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .shuffleGrouping("2Path-Spout", "P-Stream");

        builder.setBolt("2Path-BoltA-NotP",
                new TASplitSentence(() -> (long) (-Math.log(Math.random()) * 1000.0 / boltAnotP_mu)),
                ConfigUtil.getInt(conf, "2Path-BoltA-NotP.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .shuffleGrouping("2Path-Spout", "NotP-Stream");

        double boltB_mu = ConfigUtil.getDouble(conf, "2Path-BoltB.mu", 1.0);
        builder.setBolt("2Path-BoltB", new TAWordCounter(() -> (long) (-Math.log(Math.random()) * 1000.0 / boltB_mu)),
                ConfigUtil.getInt(conf, "2Path-BoltB.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .shuffleGrouping("2Path-BoltA-P")
                .shuffleGrouping("2Path-BoltA-NotP");

        conf.setNumWorkers(ConfigUtil.getInt(conf, "2Path-NumOfWorkers", 1));
        conf.setMaxSpoutPending(ConfigUtil.getInt(conf, "2Path-MaxSpoutPending", 0));
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

        StormSubmitter.submitTopology("resa-Sim-2Path-top", resaConfig, builder.createTopology());
    }
}