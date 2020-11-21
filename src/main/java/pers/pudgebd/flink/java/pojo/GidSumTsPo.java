package pers.pudgebd.flink.java.pojo;

public class GidSumTsPo {

    public Double goodsId;
    public Long sumTs;

    public GidSumTsPo() {
    }

    public GidSumTsPo(Double goodsId, Long sumTs) {
        this.goodsId = goodsId;
        this.sumTs = sumTs;
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

    @Override
    public String toString() {
        return "GidSumTsPo{" +
                "goodsId=" + goodsId +
                ", sumTs=" + sumTs +
                '}';
    }
}
