package TestTopology.helper;

import resa.util.ConfigUtil;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Created by ding on 14-3-18.
 */
public class DataSenderAdd extends DataSender {


    public DataSenderAdd(Map<String, Object> conf) {
        super(conf);
    }

    @Override
    protected String processData(String line) {
        return '+' + line;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        DataSenderAdd addSender = new DataSenderAdd(ConfigUtil.readConfig(new File(args[0])));
        runWithInstance(addSender, args);
    }

}
