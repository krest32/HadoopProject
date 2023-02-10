package com.krest.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSClient {
    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        URI uri = new URI("hdfs://hadoop100:8020");
        Configuration config = new Configuration();
        String user = "root";
        fs = FileSystem.get(uri, config, user);

    }

    @After
    public void close() throws IOException {
        fs.close();
    }

    @Test
    public void mkdir() throws IOException {
        fs.mkdirs(new Path("sanguo/huaguoshan"));
    }
}
