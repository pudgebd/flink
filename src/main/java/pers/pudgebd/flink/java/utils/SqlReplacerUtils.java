package pers.pudgebd.flink.java.utils;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.pudgebd.flink.java.pojo.SqlReplacerVo;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.SqlKind.SELECT;

/**
 * @author chenqian
 * @version 1.0.0
 */
public class SqlReplacerUtils {

    private static Logger LOG = LoggerFactory.getLogger(SqlReplacerUtils.class);

    private static final String GET_OPERAND_LIST = "getOperandList";
    private static final String FIELD_STR_FORMAT = "`%s`";

    /**
     * 1、将出现在 sql select 中的字段 x，替换为 `x`
     */
    public static String replaceCanntRecognizedByFlink(String sql) {
        try {
            String newSql = replaceSelectFields(sql);
            return newSql;
        } catch (Exception e) {
            LOG.warn("replaceCanntRecognizedByFlink error", e);
            return sql;
        }
    }


    /**
     * 如果字段本身就有``, 本方法可以正常替换，开始结束索引包括`的位置，sqlIdentity.toString 的结果不包含`
     */
    private static String replaceSelectFields(String sql) throws Exception {
        //去掉换行符等，所有sql为一行
        String newSql = sql.replaceAll("\\s+", " ").trim();
        SqlParser.ConfigBuilder configBuilder = SqlParser.configBuilder().setLex(Lex.JAVA);
        SqlNodeList sqlNodeList = SqlParser.create(newSql, configBuilder.build()).parseStmtList();
//        SqlNode sqlNode = SqlParser.create(newSql, configBuilder.build()).parseStmt();

        StringBuilder oldSqlSb = new StringBuilder(newSql);
        List<SqlReplacerVo> sqlReplacerVos = new ArrayList<>();
        for (SqlNode sqlNode : sqlNodeList.getList()) {
            recursionSqlNode(sqlNode, sqlReplacerVos, false);
        }

        int vosSize = sqlReplacerVos.size();
        if (vosSize == 0) {
            return sql;
        }

        Comparator<SqlReplacerVo> comparator = new Comparator<SqlReplacerVo>() {
            @Override
            public int compare(SqlReplacerVo o1, SqlReplacerVo o2) {
                if (o1.getStartIndex() > o2.getStartIndex()) {
                    return 1;
                } else {
                    return 0;
                }
            }
        };

        List<SqlReplacerVo> sortedVos = sqlReplacerVos.stream().sorted(comparator).collect(Collectors.toList());
        StringBuilder newSqlSb = new StringBuilder();
        int oldSqlStartIdx = 0;
        int oldSqlEndIdx = 0;
        for (SqlReplacerVo sqlReplacerVo : sortedVos) {
            oldSqlEndIdx = sqlReplacerVo.getStartIndex() - 1;
            newSqlSb
                    .append(oldSqlSb.substring(oldSqlStartIdx, oldSqlEndIdx))
                    .append(sqlReplacerVo.getReplacement());

            oldSqlStartIdx = sqlReplacerVo.getEndIndex();
        }
        return newSqlSb.toString();
    }

    private static void recursionSqlNode(
            SqlNode sqlNode, List<SqlReplacerVo> sqlReplacerVos, boolean onlyCheckSqlId) throws Exception {
        if (onlyCheckSqlId) {
            if (sqlNode.getKind() == SELECT) {
                parseSelectNode(sqlNode, sqlReplacerVos);
            } else {
                if (sqlNode instanceof SqlIdentifier) {
                    handleSqlId(sqlNode, sqlReplacerVos);

                } else if (sqlNode instanceof SqlCase) {
                    SqlCase sqlCase = (SqlCase) sqlNode;
                    List<SqlNode> sqlNodes = sqlCase.getOperandList();
                    for (SqlNode ele1 : sqlNodes) {
                        if (ele1 instanceof SqlNodeList) {
                            SqlNodeList sqlNodeList = (SqlNodeList) ele1;
                            for (SqlNode ele2 : sqlNodeList.getList()) {
                                parseOtherNode(ele2, sqlReplacerVos, onlyCheckSqlId);
                            }
                        }
                    }
                } else {
                    parseOtherNode(sqlNode, sqlReplacerVos, onlyCheckSqlId);
                }
            }
        } else {
            SqlKind sqlKind = sqlNode.getKind();
            switch (sqlKind) {
                case SELECT:
                    parseSelectNode(sqlNode, sqlReplacerVos);
                    break;
                default:
                    parseOtherNode(sqlNode, sqlReplacerVos, onlyCheckSqlId);
                    break;
            }
        }
    }

    private static void parseSelectNode(SqlNode sqlNode, List<SqlReplacerVo> sqlReplacerVos) throws Exception {
        SqlSelect sqlSelect = (SqlSelect) sqlNode;
        SqlNodeList sqlNodeList = sqlSelect.getSelectList();
        List<SqlNode> selects = sqlNodeList.getList();
        for (SqlNode seleChild : selects) {
            if (seleChild == null) {
                continue;
            }
            if (seleChild instanceof SqlIdentifier) {
                handleSqlId(seleChild, sqlReplacerVos);

            } else if (seleChild instanceof SqlCall || seleChild instanceof SqlCase) {
                recursionSqlNode(seleChild, sqlReplacerVos, true);

            } else {
                recursionSqlNode(seleChild, sqlReplacerVos, false);
            }
        }

    }

    private static void handleSqlId(SqlNode seleChild, List<SqlReplacerVo> sqlReplacerVos) {
        SqlIdentifier sqlId = (SqlIdentifier) seleChild;
        SqlParserPos sqlParserPos = sqlId.getParserPosition();
        SqlParserPos pos = sqlId.getParserPosition();
        String replacement = String.format(FIELD_STR_FORMAT, sqlId.toString());
        sqlReplacerVos.add(new SqlReplacerVo(pos.getColumnNum(), pos.getEndColumnNum(), replacement));
    }



    private static void parseOtherNode(
            SqlNode sqlNode, List<SqlReplacerVo> sqlReplacerVos, boolean onlyCheckSqlId) throws Exception {
        Method getOperandListMethod = null;
        Class clazz = sqlNode.getClass();
        try {
            getOperandListMethod = clazz.getDeclaredMethod(GET_OPERAND_LIST);
        } catch (Exception e1) {
            try {
                getOperandListMethod = clazz.getMethod(GET_OPERAND_LIST);
            } catch (Exception e2) {
                //ignore
            }
        }
        if (getOperandListMethod != null) {
            Object result = getOperandListMethod.invoke(sqlNode);
            if (result instanceof List) {
                List<SqlNode> childrenNode = (List<SqlNode>) result;
                for (SqlNode ele : childrenNode) {
                    if (ele == null) {
                        continue;
                    }
                    recursionSqlNode(ele, sqlReplacerVos, onlyCheckSqlId);
                }
            }
        }
    }


}
