package pers.pudgebd.flink.java.sql;

public class CrtTblSqls {


    public static String KAFKA_SOURCE =
            "CREATE TABLE kafka_source (\n" +
                    " id int,\n" +
                    " first VARCHAR,\n" +
                    " last STRING,\n" +
                    " user_id int,\n" +
                    " PRIMARY KEY (id) NOT ENFORCED\n" +
                    ") WITH (\n" +
                    " 'connector' = 'kafka-0.11',\n" +
                    " 'topic' = 'mytopic02',\n" +
                    " 'properties.bootstrap.servers' = 'localhost:9092',\n" +
                    " 'properties.group.id' = 'testGroup02',\n" +
                    " 'format' = 'csv',\n" +
                    " 'scan.startup.mode' = 'latest-offset'\n" +
                    ")";


    public static String DIM_MYSQL =
            "CREATE TABLE dim_mysql (\n" +
                    "  id int,\n" +
                    "  name STRING,\n" +
                    "  user_id int,\n" +
                    "  score double,\n" +
                    "  PRIMARY KEY (id) NOT ENFORCED\n" +
                    ") WITH (\n" +
                    "   'connector' = 'jdbc',\n" +
                    "   'url' = 'jdbc:mysql://localhost:3306/test',\n" +
                    "   'table-name' = 'dim_mysql',\n" +
                    "   'username' = 'root',\n" +
                    "   'password' = '123456',\n" +
                    "   'lookup.cache.max-rows' = '-1',\n" +
                    "   'lookup.cache.ttl' = '20s'" +
                    ")";


    public static String INSERT_TEST =
            "CREATE TABLE insert_test (\n" +
                    "  id int,\n" +
                    "  first STRING,\n" +
                    "  last STRING,\n" +
                    "  score double,\n" +
                    "  PRIMARY KEY (id) NOT ENFORCED\n" +
                    ") WITH (\n" +
                    "   'connector' = 'jdbc',\n" +
                    "   'url' = 'jdbc:mysql://localhost:3306/test',\n" +
                    "   'table-name' = 'insert_test',\n" +
                    "   'username' = 'root',\n" +
                    "   'password' = '123456'\n" +
                    ")";
}
