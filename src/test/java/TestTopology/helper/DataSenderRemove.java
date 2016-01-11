package TestTopology.helper;

import resa.util.ConfigUtil;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Created by ding on 14-3-18.
 */
public class DataSenderRemove extends DataSender {

    public DataSenderRemove(Map<String, Object> conf) {
        super(conf);
    }

    @Override
    protected String processData(String line) {
        return '-' + line;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        DataSenderRemove rmSender = new DataSenderRemove(ConfigUtil.readConfig(new File(args[0])));
        runWithInstance(rmSender, args);
    }
}
