package pers.pudgebd.flink.java.pojo;

public class IdNamePo {

    public String id;
    public String name;
    public long ts;

    public IdNamePo() {
    }

    public IdNamePo(String id, String name, long ts) {
        this.id = id;
        this.name = name;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "IdNamePo{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", ts=" + ts +
                '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }
}
