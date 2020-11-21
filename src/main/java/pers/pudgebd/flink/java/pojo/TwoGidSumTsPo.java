package pers.pudgebd.flink.java.pojo;

public class TwoGidSumTsPo {

    public Double goodsId;
    public Long sumTs;
    public Double goodsId2;
    public Long sumTs2;

    public TwoGidSumTsPo() {
    }

    public TwoGidSumTsPo(Double goodsId, Long sumTs, Double goodsId2, Long sumTs2) {
        this.goodsId = goodsId;
        this.sumTs = sumTs;
        this.goodsId2 = goodsId2;
        this.sumTs2 = sumTs2;
    }

    @Override
    public String toString() {
        return "TwoGidSumTsPo{" +
                "goodsId=" + goodsId +
                ", sumTs=" + sumTs +
                ", goodsId2=" + goodsId2 +
                ", sumPrice=" + sumTs2 +
                '}';
    }

    public Double getGoodsId() {
        return goodsId;
    }

    public void setGoodsId(Double goodsId) {
        this.goodsId = goodsId;
    }

    public Long getSumTs() {
        return sumTs;
    }

    public void setSumTs(Long sumTs) {
        this.sumTs = sumTs;
    }

    public Double getGoodsId2() {
        return goodsId2;
    }

    public void setGoodsId2(Double goodsId2) {
        this.goodsId2 = goodsId2;
    }

    public Long getSumTs2() {
        return sumTs2;
    }

    public void setSumTs2(Long sumTs2) {
        this.sumTs2 = sumTs2;
    }
}
