package gientech.sql;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;

/**
 * author: yhl
 * time: 2021/6/18 下午5:07
 * company: gientech
 */
public class MyblinkSqlUtils {

    public static SqlNodeList parseSql(String sql) throws Exception{
        InputStream inputStream = new ByteArrayInputStream(sql.getBytes());
        SqlParser sqlParser = getSqlParser(inputStream);
        SqlNodeList sqlNodes = sqlParser.parseStmtList();
        System.out.println("sqlNodes  is  -----");
        System.out.println(sqlNodes);
        return sqlNodes;
    }

    public static SqlParser getSqlParser(InputStream source){
        Reader reader = new InputStreamReader(source, Charset.defaultCharset());
        return SqlParser.create(reader,
                SqlParser.configBuilder()
                        .setParserFactory(FlinkSqlParserImpl.FACTORY)
                        .setQuoting(Quoting.BACK_TICK)
                        .setUnquotedCasing(Casing.UNCHANGED)
                        .setQuotedCasing(Casing.UNCHANGED)
                        .setConformance(FlinkSqlConformance.HIVE)
                        .build());
    }

    public static void main(String[] args) throws Exception {
        String table = "CREATE TABLE tbl1 (\n" +
                "  a bigint,\n" +
                "  h varchar, \n" +
                "  g as 2 * (a + 1), \n" +
                "  ts as toTimestamp(b, 'yyyy-MM-dd HH:mm:ss'), \n" +
                "  b varchar,\n" +
                "  proc as PROCTIME(), \n" +
                "  PRIMARY KEY (a, b)\n" +
                ")\n" +
                "with (\n" +
                "    'connector' = 'kafka', \n" +
                "    'kafka.topic' = 'log.test'\n" +
                ")\n";

        SqlNodeList sqlNodes = MyblinkSqlUtils.parseSql(table);
        for(SqlNode sqlNode:sqlNodes){
            System.out.println("a");
        }
    }
}
