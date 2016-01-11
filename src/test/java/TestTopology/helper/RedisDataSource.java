package TestTopology.helper;

import org.codehaus.jackson.map.ObjectMapper;
import redis.clients.jedis.Jedis;
import resa.metrics.MeasuredData;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by ding on 14-5-2.
 */
public class RedisDataSource {

    public static List<MeasuredData> readData(String host, int port, String queue, int maxLen) {
        ObjectMapper objectMapper = new ObjectMapper();
        Jedis jedis = new Jedis(host, port);
        List<MeasuredData> ret = new ArrayList<>();
        try {
            String line = null;
            int count = 0;
            while ((line = jedis.lpop(queue)) != null && count++ < maxLen) {
                String[] tmp = line.split("->");
                String[] head = tmp[0].split(":");
                ret.add(new MeasuredData(head[0], Integer.valueOf(head[1]), System.currentTimeMillis(),
                        objectMapper.readValue(tmp[1], Map.class)));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jedis.disconnect();
        }
        return ret;
    }

    public static List<MeasuredData> iterData(String host, int port, String queue, int maxLen) {
        ObjectMapper objectMapper = new ObjectMapper();
        Jedis jedis = new Jedis(host, port);
        List<MeasuredData> ret = new ArrayList<>();
        try {
            Iterator<String> strings = jedis.lrange(queue, 0, maxLen - 1).iterator();
            String line;
            int count = 0;
            while (strings.hasNext() && count++ < maxLen) {
                line = strings.next();
                String[] tmp = line.split("->");
                String[] head = tmp[0].split(":");
                ret.add(new MeasuredData(head[0], Integer.valueOf(head[1]), System.currentTimeMillis(),
                        objectMapper.readValue(tmp[1], Map.class)));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jedis.disconnect();
        }
        return ret;
    }


    public static void writeData2File(String host, int port, String queue, int maxLen, String outputFile) {
        Jedis jedis = new Jedis(host, port);
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFile))) {
            String line = null;
            int count = 0;
            while ((line = jedis.lpop(queue)) != null && count++ < maxLen) {
                writer.append(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jedis.disconnect();
        }
    }

    public static List<MeasuredData> readData(String host, int port, String queue) {
        return readData(host, port, queue, Integer.MAX_VALUE);
    }

    public static void main(String[] args) {
        writeData2File(args[0], Integer.parseInt(args[1]), args[2], Integer.parseInt(args[3]), args[4]);
    }

    public static void clearQueue(String host, int port, String queue) {
        Jedis jedis = new Jedis(host, port);
        try {
            jedis.del(queue);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jedis.disconnect();
        }
    }
}
