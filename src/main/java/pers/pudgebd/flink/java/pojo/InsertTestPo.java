package pers.pudgebd.flink.java.pojo;

public class InsertTestPo {

    public String first;
    public String last;
    public Double score;

    public InsertTestPo() {
    }

    public InsertTestPo(String first, String last, Double score) {
        this.first = first;
        this.last = last;
        this.score = score;
    }

    @Override
    public String toString() {
        return super.toString();
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

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }
}
