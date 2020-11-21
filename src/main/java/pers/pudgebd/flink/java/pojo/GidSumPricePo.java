package pers.pudgebd.flink.java.pojo;

public class GidSumPricePo {

    public Double goodsId;
    public Double sumPrice;

    public GidSumPricePo() {
    }

    public GidSumPricePo(Double goodsId, Double sumPrice) {
        this.goodsId = goodsId;
        this.sumPrice = sumPrice;
    }

    public Double getGoodsId() {
        return goodsId;
    }

    public void setGoodsId(Double goodsId) {
        this.goodsId = goodsId;
    }

    public Double getSumPrice() {
        return sumPrice;
    }

    public void setSumPrice(Double sumPrice) {
        this.sumPrice = sumPrice;
    }

    @Override
    public String toString() {
        return "GidSumPricePo{" +
                "goodsId=" + goodsId +
                ", sumPrice=" + sumPrice +
                '}';
    }

}
