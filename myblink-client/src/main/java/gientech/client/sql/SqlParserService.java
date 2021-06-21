package gientech.client.sql;

import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.dml.RichSqlInsert;

import java.util.List;

/**
 * author: yhl
 * time: 2021/6/18 下午4:57
 * company: gientech
 */
public interface SqlParserService {
    List<SqlCreateFunction> sqlFunctionParser(String funSql) throws Exception;
    List<SqlCreateView> sqlViewParser(String viewSql) throws Exception;
    List<SqlCreateTable> sqlTableParser(String tableSql) throws Exception;
    List<RichSqlInsert> sqlInsertParser(String insertSql) throws Exception;
}
