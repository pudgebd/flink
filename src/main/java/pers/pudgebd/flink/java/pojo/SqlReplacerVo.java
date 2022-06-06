package pers.pudgebd.flink.java.pojo;

public class SqlReplacerVo {

    private int startIndex;
    private int endIndex;
    private String replacement;

    public SqlReplacerVo(int startIndex, int endIndex, String replacement) {
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.replacement = replacement;
    }

    public int getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(int startIndex) {
        this.startIndex = startIndex;
    }

    public int getEndIndex() {
        return endIndex;
    }

    public void setEndIndex(int endIndex) {
        this.endIndex = endIndex;
    }

    public String getReplacement() {
        return replacement;
    }

    public void setReplacement(String replacement) {
        this.replacement = replacement;
    }
}
