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
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

/**
 * Created by ding on 14-3-18.
 */
public class DataSenderWithTS {

    private String host;
    private int port;
    private String queueName;
    private int maxPaddingSize;

    public DataSenderWithTS(Map<String, Object> conf) {
        this.host = (String) conf.get("redis.host");
        this.port = ((Number) conf.get("redis.port")).intValue();
        this.queueName = (String) conf.get("redis.sourceQueueName");
    }

    public void send2Queue(Path inputFile, LongSupplier sleep) throws IOException {
        Jedis jedis = new Jedis(host, port);
        int counter = 0;
        try (BufferedReader reader = Files.newBufferedReader(inputFile)) {
            String line = null;
            while (line != null || (line = reader.readLine()) != null) {
                long ms = sleep.getAsLong();
                if (ms > 0) {
                    Utils.sleep(ms);
                }
                if (jedis.llen(queueName) < maxPaddingSize) {
                    String data = counter++ + "|" + System.currentTimeMillis() + "|" + line;
                    jedis.rpush(queueName, data);
                    line = null;
                }
            }
        } finally {
            jedis.quit();
        }
    }

    public void send2Queue(Path inputFile, LongSupplier sleep, IntSupplier sendCnt) throws IOException {
        Jedis jedis = new Jedis(host, port);
        int counter = 0;
        try (BufferedReader reader = Files.newBufferedReader(inputFile)) {
            String line = null;
            int toSendCnt = 0;
            while (line != null || (line = reader.readLine()) != null) {
                if (toSendCnt > 0) {
                    toSendCnt--;
                } else {
                    long ms = sleep.getAsLong();
                    if (ms > 0) {
                        Utils.sleep(ms);
                    }
                    toSendCnt = sendCnt.getAsInt();
                }

                if (jedis.llen(queueName) < maxPaddingSize) {
                    String data = counter++ + "|" + System.currentTimeMillis() + "|" + line;
                    jedis.rpush(queueName, data);
                    line = null;
                }
            }
        } finally {
            jedis.quit();
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 6) {
            System.out.println("usage: DataSender <confFile> <inputFile> <maxPaddingSize> " +
                    "<rateForSplitThreash> <rateForSplit> " +
                    "[-deter <rate>] [-poison <lambda>] [-uniform <left> <right>]");
            return;
        }
        DataSenderWithTS sender = new DataSenderWithTS(ConfigUtil.readConfig(new File(args[0])));
        System.out.println("start sender");
        Path dataFile = Paths.get(args[1]);
        int maxPadding = Integer.parseInt(args[2]);
        sender.maxPaddingSize = maxPadding > 0 ? maxPadding : Integer.MAX_VALUE;
        int batchRateThreash = Integer.parseInt(args[3]);
        int batchRate = Integer.parseInt(args[4]);
        switch (args[5].substring(1)) {
            case "deter":
                double rate = Float.parseFloat(args[6]);
                if (rate < batchRateThreash) {
                    System.out.println("case Det, rate: " + rate);
                    sender.send2Queue(dataFile, () -> (long) (1000 / rate));
                } else {
                    double meanBatchSize = rate / batchRate;
                    System.out.println("case Det, rate: " + rate + "meanBatchSize: " + meanBatchSize);
                    sender.send2Queue(dataFile, () -> (long) (1000 / batchRate),
                            ///() -> (int) (Math.random() * 2.0 * meanBatchSize));
                            () -> (int) meanBatchSize);
                }
                break;
            case "poison":
                double lambda = Float.parseFloat(args[6]);
                if (lambda < batchRateThreash) {
                    System.out.println("case Poison, lambda: " + lambda);
                    sender.send2Queue(dataFile, () -> (long) (-Math.log(Math.random()) * 1000 / lambda));
                } else {
                    double meanBatchSize = lambda / batchRate;
                    System.out.println("case Poison, lambda: " + lambda + "meanBatchSize: " + meanBatchSize);
                    sender.send2Queue(dataFile, () -> (long) (-Math.log(Math.random()) * 1000 / batchRate),
                            () -> (int) (Math.random() * 2.0 * meanBatchSize));
                }
                break;
            case "uniform":
                if (args.length < 8) {
                    System.out.println("usage: DataSender <confFile> <inputFile> <maxPaddingSize>" +
                            " [-deter <rate>] [-poison <lambda>] [-uniform <left> <right>]");
                    return;
                }
                double left = Float.parseFloat(args[6]);
                double right = Float.parseFloat(args[7]);
                sender.send2Queue(dataFile, () -> (long) (1000 / (Math.random() * (right - left) + left)));
            default:
                System.out.println("usage: DataSender <confFile> <inputFile> <maxPaddingSize> " +
                        "<rateForSplitThreash> <rateForSplit> " +
                        "[-deter <rate>] [-poison <lambda>] [-uniform <left> <right>]");
                break;
        }
        System.out.println("end sender");
    }

}
