package pers.pudgebd.flink.java.hive.pojo;

public class HiveDimPojo {

    public String channel;
    public String name;

    public HiveDimPojo() {
    }

    public HiveDimPojo(String channel, String name) {
        this.channel = channel;
        this.name = name;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
