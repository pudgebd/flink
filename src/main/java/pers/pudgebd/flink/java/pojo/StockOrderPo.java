package pers.pudgebd.flink.java.pojo;

public class StockOrderPo {

    public String acct_id;
    public String sec_code;
    public Long order_vol;
    public Long ts;

    public StockOrderPo() {
    }

    public StockOrderPo(String acct_id, String sec_code, Long order_vol, Long ts) {
        this.acct_id = acct_id;
        this.sec_code = sec_code;
        this.order_vol = order_vol;
        this.ts = ts;
    }

    public String getAcct_id() {
        return acct_id;
    }

    public void setAcct_id(String acct_id) {
        this.acct_id = acct_id;
    }

    public String getSec_code() {
        return sec_code;
    }

    public void setSec_code(String sec_code) {
        this.sec_code = sec_code;
    }

    public Long getOrder_vol() {
        return order_vol;
    }

    public void setOrder_vol(Long order_vol) {
        this.order_vol = order_vol;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "StockOrderPo{" +
                "acct_id='" + acct_id + '\'' +
                ", sec_code='" + sec_code + '\'' +
                ", order_vol=" + order_vol +
                ", ts=" + ts +
                '}';
    }
}
