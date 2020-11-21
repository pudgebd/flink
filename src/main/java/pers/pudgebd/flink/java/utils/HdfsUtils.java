package pers.pudgebd.flink.java.utils;


import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.stream.Collectors;


/**
 * v1
 */
public class HdfsUtils {

    private static Configuration conf = new Configuration();
    private static FileSystem fs = null;

    public static String default_address = "hdfs://cdh601:8020";
    public static String default_username = "work";

    public HdfsUtils() throws Exception{
        fs = FileSystem.get(new URI(default_address), conf, default_username);
    }

    public HdfsUtils(String address, String username) throws Exception{
        fs = FileSystem.get(new URI(address), conf, username);
    }

    public static FileSystem getFs() throws Exception {
        if (fs == null) {
            fs = FileSystem.get(new URI(default_address), conf, default_username);
        }
        return fs;
    }

    public boolean Exists(String dir) throws Exception{
        return fs.exists(new Path(dir));
    }

    //列出指定目录下所有文件 包含目录
    public void listDir(String path) throws Exception{
        FileStatus[] files = fs.listStatus(new Path(path));
        for(FileStatus f : files){
            System.out.println(f);
        }
    }

    //列出指定目录下所有文件 不包含目录 非递归
    public void listFile(String path,boolean recursive) throws Exception{
        RemoteIterator<LocatedFileStatus> itr = fs.listFiles(new Path(path), recursive);
        while(itr.hasNext()){
            System.out.println(itr.next());
        }
    }

    //列出指定目录下所有文件 不包含目录 递归
    public void listFile(String path) throws Exception{
        RemoteIterator<LocatedFileStatus> itr = fs.listFiles(new Path(path), false);
        while(itr.hasNext()){
            System.out.println(itr.next());
        }
    }


    //
    public void createFile(String path,byte[] contents) throws Exception{
        FSDataOutputStream f = fs.create(new Path(path));
        f.write(contents);
        f.close();
    }

    public void createFile(String filePath, String contents) throws Exception{
        createFile(filePath,contents.getBytes());
    }


    public void copyFromLocalFile(String src, String dst, boolean overwrite) throws Exception{
        File srcFile = new File(src);
        if (srcFile.isDirectory()) {
            File[] files = srcFile.listFiles();
            Path[] paths = Arrays.stream(files).map(file -> new Path(file.getAbsolutePath()))
                    .collect(Collectors.toList())
                    .toArray(new Path[files.length]);

            fs.copyFromLocalFile(false, overwrite, paths, new Path(dst));
        } else {
            fs.copyFromLocalFile(new Path(src), new Path(dst));
        }
    }

    //mkdir
    public boolean mkdir(String dir) throws Exception{
        boolean isSuccess = fs.mkdirs(new Path(dir));
        return isSuccess;
    }

    //rm
    public boolean delete(String filePath,boolean recursive) throws Exception{
        boolean isSuccess = fs.delete(new Path(filePath), recursive);
        return isSuccess;
    }

    //rm -r
    public boolean delete(String filePath) throws Exception{
        boolean isSuccess = fs.delete(new Path(filePath), true);
        return isSuccess;
    }

    public void rename(String oldName, String newName) throws Exception{
        fs.rename(new Path(oldName), new Path(newName));
    }

    //
    public String readFile(String filePath) throws Exception{
        String content = null;
        InputStream in = null;
        ByteArrayOutputStream out = null;

        try{
            in = fs.open(new Path(filePath));
            out = new ByteArrayOutputStream(in.available());
            IOUtils.copyBytes(in, out, conf);
            content = out.toString();
        }finally{
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }

        return content;
    }



}
