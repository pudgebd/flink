package pers.pudgebd.flink.java.pojo;

public class KvPo {

    public String key;
    public String val;

    public KvPo() {
    }

    public KvPo(String key, String val) {
        this.key = key;
        this.val = val;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getVal() {
        return val;
    }

    public void setVal(String val) {
        this.val = val;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
