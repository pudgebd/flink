package test;

import org.junit.Test;
import pers.pudgebd.flink.java.utils.HdfsUtils;

public class HdfsTest {

    @Test
    public void listDir() throws Exception {
        HdfsUtils hdfsUtils = new HdfsUtils();
        hdfsUtils.listDir("/flink-dist/flink-1.11.2");
    }


    @Test
    public void mkdir() throws Exception {
        HdfsUtils hdfsUtils = new HdfsUtils();
        hdfsUtils.mkdir("/flink-dist/platform-jars/flink-1.11-scala-2.11");
    }


    @Test
    public void put() throws Exception {
        HdfsUtils hdfsUtils = new HdfsUtils();
        hdfsUtils.copyFromLocalFile(
                "/Users/pudgebd/tools/flinkkkkkkkkkkkkkkkkkk/platform-jars/flink-1.11-scala-2.12",
                "/flink-dist/platform-jars/flink-1.11-scala-2.12",
                true);
    }


    @Test
    public void delete() throws Exception {
        HdfsUtils hdfsUtils = new HdfsUtils();
        hdfsUtils.delete("/flink-dist/platform-jars/1.11/common/flink.udf-1.0-jar-with-dependencies.jar");
    }


}
