package pers.pudgebd.flink.java.pojo;

public class KafkaSrcPo {

    public Integer id;
    public String first;
    public String last;
    public Integer user_id;

    public KafkaSrcPo() {
    }

    public KafkaSrcPo(Integer id, String first, String last, Integer user_id) {
        this.id = id;
        this.first = first;
        this.last = last;
        this.user_id = user_id;
    }

    @Override
    public String toString() {
        return super.toString();
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public String getLast() {
        return last;
    }

    public void setLast(String last) {
        this.last = last;
    }

    public Integer getUser_id() {
        return user_id;
    }

    public void setUser_id(Integer user_id) {
        this.user_id = user_id;
    }
}
