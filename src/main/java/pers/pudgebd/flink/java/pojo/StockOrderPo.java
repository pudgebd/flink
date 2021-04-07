package pers.pudgebd.flink.java.pojo;

public class StockOrderPo {

    public String acct_id;
    public String sec_code;
    public Long ts;

    public StockOrderPo() {
    }

    public StockOrderPo(String acct_id, String sec_code, Long ts) {
        this.acct_id = acct_id;
        this.sec_code = sec_code;
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

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }
}
