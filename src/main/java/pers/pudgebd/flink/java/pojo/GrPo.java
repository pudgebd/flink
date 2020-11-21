package pers.pudgebd.flink.java.pojo;

/**
 * 还是要报错
 * Null result cannot be stored in a Case Class.
 */
public class GrPo {

    public Double goodsId;
    public Double userId;
    public Double raiting;
    public Double viewCounts;
    public Double stayMs;
    public Double isStar;
    public Double buyCounts;
    public Long ts;

    public GrPo() {
    }

    public GrPo(Double goodsId, Double userId, Double raiting, Double viewCounts, Double stayMs,
                Double isStar, Double buyCounts, Long ts) {
        this.goodsId = goodsId;
        this.userId = userId;
        this.raiting = raiting;
        this.viewCounts = viewCounts;
        this.stayMs = stayMs;
        this.isStar = isStar;
        this.buyCounts = buyCounts;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "GrPo{" +
                "goodsId=" + goodsId +
                ", ts=" + ts +
                '}';
    }

    public Double getGoodsId() {
        return goodsId;
    }

    public void setGoodsId(Double goodsId) {
        this.goodsId = goodsId;
    }

    public Double getUserId() {
        return userId;
    }

    public void setUserId(Double userId) {
        this.userId = userId;
    }

    public Double getRaiting() {
        return raiting;
    }

    public void setRaiting(Double raiting) {
        this.raiting = raiting;
    }

    public Double getViewCounts() {
        return viewCounts;
    }

    public void setViewCounts(Double viewCounts) {
        this.viewCounts = viewCounts;
    }

    public Double getStayMs() {
        return stayMs;
    }

    public void setStayMs(Double stayMs) {
        this.stayMs = stayMs;
    }

    public Double getIsStar() {
        return isStar;
    }

    public void setIsStar(Double isStar) {
        this.isStar = isStar;
    }

    public Double getBuyCounts() {
        return buyCounts;
    }

    public void setBuyCounts(Double buyCounts) {
        this.buyCounts = buyCounts;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }
}
