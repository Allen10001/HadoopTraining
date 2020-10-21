package com.letv.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class testCopyFromLocalFile {
    @Test
    public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), configuration, "allen");

        // 2 上传文件
        fs.copyFromLocalFile(new Path("/Users/allen/bigdataapp/HadoopTraining/AlbumCount/input/works_album_info.txt"), new Path("hdfsfilepath/works_album_info.txt"));

        // 3 关闭资源
        fs.close();

        System.out.println("over");
    }
}


