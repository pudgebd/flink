package pers.pudgebd.flink.java.pojo;

public class GrHivePo {

    public Long goods_id;
    public String mid;
    public Double raiting;

    public GrHivePo() {
    }

    public GrHivePo(Long goods_id, String mid, Double raiting) {
        this.goods_id = goods_id;
        this.mid = mid;
        this.raiting = raiting;
    }


    public Long getGoods_id() {
        return goods_id;
    }

    public void setGoods_id(Long goods_id) {
        this.goods_id = goods_id;
    }

    public String getMid() {
        return mid;
    }

    public void setMid(String mid) {
        this.mid = mid;
    }

    public Double getRaiting() {
        return raiting;
    }

    public void setRaiting(Double raiting) {
        this.raiting = raiting;
    }
}
