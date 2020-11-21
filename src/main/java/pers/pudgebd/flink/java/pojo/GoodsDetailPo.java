package pers.pudgebd.flink.java.pojo;


/**
 * 还是要报错
 * Null result cannot be stored in a Case Class.
 */
public class GoodsDetailPo {

    public Double goodsId;
    public String description;
    public Double price;


    public GoodsDetailPo() {
    }

    public GoodsDetailPo(Double goodsId, String description, Double price) {
        this.goodsId = goodsId;
        this.description = description;
        this.price = price;
    }

    public Double getGoodsId() {
        return goodsId;
    }

    public void setGoodsId(Double goodsId) {
        this.goodsId = goodsId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }
}
