package TestTopology.simulated;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import resa.util.ConfigUtil;

import java.io.File;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class SimExpServWC2Path {

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.out.println("Enter path to config file!");
            System.exit(0);
        }
        Config conf = ConfigUtil.readConfig(new File(args[0]));

        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[0]);
        }

        TopologyBuilder builder = new TopologyBuilder();

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

        StormSubmitter.submitTopology("sim-2Path-top-1", conf, builder.createTopology());
    }
}
