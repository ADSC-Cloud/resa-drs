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
public class SimExpServWCLoop {

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

        builder.setSpout("loop-Spout", new TASentenceSpout(host, port, queueName),
                ConfigUtil.getInt(conf, "loop-Spout.parallelism", 1));

        double loopBoltA_mu = ConfigUtil.getDouble(conf, "loop-BoltA.mu", 1.0);
        builder.setBolt("loop-BoltA", new TASplitSentence(() -> (long) (-Math.log(Math.random()) * 1000.0 / loopBoltA_mu)),
                ConfigUtil.getInt(conf, "loop-BoltA.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .shuffleGrouping("loop-Spout");

        double loopBoltB_mu = ConfigUtil.getDouble(conf, "loop-BoltB.mu", 1.0);
        builder.setBolt("loop-BoltB",
                new TAWordCounter2Path(() -> (long) (-Math.log(Math.random()) * 1000.0 / loopBoltB_mu),
                        ConfigUtil.getDouble(conf, "loop-BoltB-loopback.prob", 0.0)),
                ConfigUtil.getInt(conf, "loop-BoltB.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .shuffleGrouping("loop-BoltA")
                .shuffleGrouping("loop-BoltB", "P-Stream");

        conf.setNumWorkers(ConfigUtil.getInt(conf, "loop-NumOfWorkers", 1));
        conf.setMaxSpoutPending(ConfigUtil.getInt(conf, "loop-MaxSpoutPending", 0));
        conf.setDebug(ConfigUtil.getBoolean(conf, "DebugTopology", false));
        conf.setStatsSampleRate(ConfigUtil.getDouble(conf, "StatsSampleRate", 1.0));

        StormSubmitter.submitTopology("sim-loop-top", conf, builder.createTopology());
    }
}
