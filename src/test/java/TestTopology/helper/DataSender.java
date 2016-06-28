package TestTopology.helper;

import org.apache.storm.utils.Utils;
import redis.clients.jedis.Jedis;
import resa.util.ConfigUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.LongSupplier;

/**
 * Created by ding on 14-3-18.
 */
public class DataSender {

    private static final String END = new String("end");

    private String host;
    private int port;
    private String queueName;

    public DataSender(Map<String, Object> conf) {
        this.host = (String) conf.get("redis.host");
        this.port = ((Number) conf.get("redis.port")).intValue();
        this.queueName = (String) conf.get("redis.queue");
    }

    private class PushThread extends Thread {

        private BlockingQueue<String> dataQueue;
        private Jedis jedis = new Jedis(host, port);

        private PushThread(BlockingQueue<String> dataQueue) {
            this.dataQueue = dataQueue;
        }

        @Override
        public void run() {
            String line;
            try {
                while ((line = dataQueue.take()) != END) {
                    jedis.rpush(queueName, line);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                dataQueue.offer(END);
            }
        }
    }

    public void send2Queue(Path inputFile, int batchSize, LongSupplier sleep) throws IOException, InterruptedException {
        BlockingQueue<String> dataQueue = new ArrayBlockingQueue<>(10000);
        for (int i = 0; i < 1; i++) {
            new PushThread(dataQueue).start();
        }
        try (BufferedReader reader = Files.newBufferedReader(inputFile)) {
            String line;
            int batchCnt = 0;
            while ((line = reader.readLine()) != null) {
                dataQueue.put(processData(line));
                if (++batchCnt == batchSize) {
                    batchCnt = 0;
                    long ms = sleep.getAsLong();
                    if (ms > 0) {
                        Utils.sleep(ms);
                    }
                }
            }
        } finally {
            dataQueue.put(END);
        }
    }

    protected String processData(String line) {
        return line;
    }


    private static void printUsage() {
        System.out.println("usage: DataSender <confFile> <inputFile> <batchSize>" +
                " [-deter <rate>] [-poison <lambda>] [-uniform <left> <right>]");
    }

    protected static void runWithInstance(DataSender sender, String[] args) throws IOException, InterruptedException {
        if (args.length < 4) {
            printUsage();
            return;
        }
        int batchSize = Integer.parseInt(args[2]);
        System.out.println("start sender");
        Path dataFile = Paths.get(args[1]);
        switch (args[3].substring(1)) {
            case "deter":
                long sleep = (long) (1000 / Float.parseFloat(args[4]));
                sender.send2Queue(dataFile, batchSize, () -> sleep);
                break;
            case "poison":
                double lambda = Float.parseFloat(args[4]);
                sender.send2Queue(dataFile, batchSize, () -> (long) (-Math.log(Math.random()) * 1000 / lambda));
                break;
            case "uniform":
                if (args.length < 6) {
                    printUsage();
                    return;
                }
                double left = Float.parseFloat(args[4]);
                double right = Float.parseFloat(args[5]);
                sender.send2Queue(dataFile, batchSize, () -> (long) (1000 / (Math.random() * (right - left) + left)));
                break;
            default:
                printUsage();
                break;
        }
        System.out.println("end sender");
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        DataSender sender = new DataSender(ConfigUtil.readConfig(new File(args[0])));
        runWithInstance(sender, args);
    }

}
