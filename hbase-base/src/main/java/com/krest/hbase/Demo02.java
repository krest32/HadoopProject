package com.krest.hbase;


import org.junit.Test;

import java.io.IOException;

import static com.krest.hbase.HBaseDML.*;

public class Demo02 {

    @Test
    public void testPut() throws IOException {
        putCell("bigdata", "student", "1002", "info", "name", "lisi");
    }


    @Test
    public void testget() throws IOException {
        getCells("bigdata", "student", "1001", "info", "name");
    }


    @Test
    public void testScan() throws IOException {
        scanRows("bigdata", "student", "1001", "2000");
    }

    @Test
    public void testDel() throws IOException {
        deleteColumn("bigdata", "student", "1001", "info", "name");
    }


}
