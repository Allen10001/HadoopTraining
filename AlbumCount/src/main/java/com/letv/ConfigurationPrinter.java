package com.letv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Map;

public class ConfigurationPrinter extends Configured implements Tool {

    static{
        // 添加系统默认资源
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
        Configuration.addDefaultResource("yarn-default.xml");
        Configuration.addDefaultResource("yarn-site.xml");
        Configuration.addDefaultResource("mapred-default.xml");
        Configuration.addDefaultResource("mapred-site.xml");
    }

    public int run(String[] args) throws Exception{
        Configuration conf = getConf();
        for(Map.Entry<String,String> entry : conf){
            System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
        }
        return 0;
    }

    public static void main(String[] args) throws Exception{
        // ConfigurationPrinter main() 方法中不直接调用自身的run(), 而是调用 ToolRunner 的run() 方法，该方法在
        // 调用 ConfigurationPrinter 的run 方法之前为 Tool 建立一个 Configuration 对象
        int exitCode = ToolRunner.run(new ConfigurationPrinter(), args);
        System.exit(exitCode);
    }
}
