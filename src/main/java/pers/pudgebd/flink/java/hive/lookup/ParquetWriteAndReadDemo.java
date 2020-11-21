package pers.pudgebd.flink.java.hive.lookup;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.*;

import java.io.IOException;


public class ParquetWriteAndReadDemo {

    //这是一种parse String来制造MessageType的办法
    private static Logger logger = Logger.getLogger(ParquetWriteAndReadDemo.class);
    private static String schemaString = "message schema {" + "optional int64 log_id;"
            + "optional binary idc_id;" + "optional int64 house_id;"
            + "optional int64 src_ip_long;" + "optional int64 dest_ip_long;"
            + "optional int64 src_port;" + "optional int64 dest_port;"
            + "optional int32 protocol_type;" + "optional binary url64;"
            + "optional binary access_time;}";
    private static MessageType schema = MessageTypeParser.parseMessageType(schemaString);

    private static MessageType getMessageTypeFromCode (){
        MessageType messageType = Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("id")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("name")
                .required(PrimitiveType.PrimitiveTypeName.INT32).named("age")
                .requiredGroup()
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("test1")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("test2")
                .named("group1")
                .named("trigger");
        //System.out.println(messageType.toString());
        return messageType;
    }

    private static void writeParquetOnDisk(String fileName){

        //1、声明parquet的messageType
        MessageType messageType = getMessageTypeFromCode();

        //2、声明parquetWriter
        Path path = new Path("/tmp/"+fileName);
        System.out.println(path);
        Configuration configuration = new Configuration();
        GroupWriteSupport.setSchema(messageType,configuration);
        GroupWriteSupport writeSupport = new GroupWriteSupport();

        //3、写数据
        ParquetWriter<Group> writer = null;
        try{
            writer = new ParquetWriter<Group>(path,
                    ParquetFileWriter.Mode.OVERWRITE,
                    writeSupport,
                    CompressionCodecName.UNCOMPRESSED,
                    128*1024*1024,
                    5*1024*1024,
                    5*1024*1024,
                    ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                    ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                    ParquetWriter.DEFAULT_WRITER_VERSION,
                    configuration);

            //4、构建parquet数据，封装成Group。
            for(int i = 0; i < 10; i ++){
                Group group = new SimpleGroupFactory(messageType).newGroup();
                group.append("name",i+"@ximalaya.com")
                        .append("id",i+"@id")
                        .append("age",i)
                        .addGroup("group1")
                        .append("test1","test1"+i)
                        .append("test2","test2"+i);
                writer.write(group);
            }
        }catch(IOException ioe){
            ioe.printStackTrace();
        }finally{
            if(writer != null) {
                try{
                    writer.close();
                }catch(IOException ioe){
                    ioe.printStackTrace();
                }
            }
        }
    }

    private static void readParquetFromDisk(String fileName){

        //1、声明readSupport
        GroupReadSupport groupReadSupport = new GroupReadSupport();
        Path path = new Path("/tmp/"+fileName);

        //2、通过parquetReader读文件
        ParquetReader<Group> reader = null;
        try{
            reader = ParquetReader.builder(groupReadSupport,path).build();
            Group group = null;
            while((group = reader.read()) !=null){
                System.out.println(group);
            }
        }catch(IOException ioe){
            ioe.printStackTrace();
        }finally{
            try{
                reader.close();
            }catch(IOException ioe){
                ioe.printStackTrace();
            }
        }
    }

    public static void writeParquetToHDFS(String ipAddr, String port, String filePath,String fileName){

        //1、声明parquet的messageType
        MessageType messageType = getMessageTypeFromCode();

        //2、声明parquetWriter
        Path path = new Path("hdfs://"+ipAddr+":"+port+"/"+filePath+"/"+fileName);
        System.out.println(path);
        Configuration configuration = new Configuration();
        GroupWriteSupport.setSchema(messageType,configuration);
        GroupWriteSupport writeSupport = new GroupWriteSupport();

        //3、写数据
        ParquetWriter<Group> writer = null;
        try{
            writer = new ParquetWriter<Group>(path,
                    ParquetFileWriter.Mode.OVERWRITE,
                    writeSupport,
                    CompressionCodecName.UNCOMPRESSED,
                    128*1024*1024,
                    5*1024*1024,
                    5*1024*1024,
                    ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                    ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                    ParquetWriter.DEFAULT_WRITER_VERSION,
                    configuration);

            //4、构建parquet数据，封装成Group。
            for(int i = 0; i < 10; i ++){
                Group group = new SimpleGroupFactory(messageType).newGroup();
                group.append("name",i+"@ximalaya.com")
                        .append("id",i+"@id")
                        .append("age",i)
                        .addGroup("group1")
                        .append("test1","test1"+i)
                        .append("test2","test2"+i);
                writer.write(group);
            }
        }catch(IOException ioe){
            ioe.printStackTrace();
        }finally{
            if(writer != null) {
                try{
                    writer.close();
                }catch(IOException ioe){
                    ioe.printStackTrace();
                }
            }
        }
    }

    public static void readParquetFromHDFS(String ipAddr, String port, String filePath,String fileName){
        //1、声明readSupport
        GroupReadSupport groupReadSupport = new GroupReadSupport();
        Path path = new Path("hdfs://"+ipAddr+":"+port+"/"+filePath+"/"+fileName);

        //2、通过parquetReader读文件
        ParquetReader<Group> reader = null;
        try{
            reader = ParquetReader.builder(groupReadSupport,path).build();
            Group group = null;
            while((group = reader.read()) !=null){
                System.out.println(group.getString("channel", 0));
                System.out.println(group.getString("name", 0));
            }
        }catch(IOException ioe){
            ioe.printStackTrace();
        }finally{
            try{
                reader.close();
            }catch(IOException ioe){
                ioe.printStackTrace();
            }
        }
    }

    public static void main(String[] args){
        //writeParquetOnDisk("test1.parq");
//        readParquetFromDisk("test1.parq");
//        writeParquetToHDFS("127.0.0.1","9000","/tmp","test1.parq");
        readParquetFromHDFS("cdh601","8020",
                "/user/hive/warehouse/myhivedimtable/dt=2020-11-16","000000_0");
    }

}
