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
public class SimExpServWC {


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

        StormSubmitter.submitTopology("sim-chain-top", conf, builder.createTopology());
    }
}
