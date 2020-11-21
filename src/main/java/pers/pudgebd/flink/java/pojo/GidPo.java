package pers.pudgebd.flink.java.pojo;

public class GidPo {

    public Double goodsId;

    public GidPo() {
    }

    public GidPo(Double goodsId) {
        this.goodsId = goodsId;
    }

    @Override
    public String toString() {
        return "GidPo{" +
                "goodsId=" + goodsId +
                '}';
    }

    public Double getGoodsId() {
        return goodsId;
    }

    public void setGoodsId(Double goodsId) {
        this.goodsId = goodsId;
    }
}
