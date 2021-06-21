package gientech.sql.impl;

import gientech.client.sql.SqlParserService;
import gientech.sql.MyblinkSqlUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.*;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.dml.RichSqlInsert;

import java.util.*;

/**
 * author: yhl
 * time: 2021/6/18 下午5:14
 * company: gientech
 */
@Slf4j
public class SqlParserServiceImpl implements SqlParserService {

    @Override
    public List<SqlCreateFunction> sqlFunctionParser(String funSql) throws Exception {
        List<SqlCreateFunction> funcList = new LinkedList<>();
        SqlNodeList sqlNodes = MyblinkSqlUtils.parseSql(funSql);
        for(SqlNode sqlNode:sqlNodes){
            if(sqlNode instanceof SqlCreateFunction){
                funcList.add((SqlCreateFunction) sqlNode);
            }
        }

        return funcList;
    }

    @Override
    public List<SqlCreateView> sqlViewParser(String viewSql) throws Exception {
        List<SqlCreateView> viewList= new LinkedList<>();
        SqlNodeList sqlNodes = MyblinkSqlUtils.parseSql(viewSql);
        for(SqlNode sqlNode:sqlNodes){
            if(sqlNode instanceof SqlCreateView){
                viewList.add((SqlCreateView) sqlNode);
            }
        }
        return viewList;
    }

    @Override
    public List<SqlCreateTable> sqlTableParser(String tableSql) throws Exception {
        List<SqlCreateTable> tableList = new LinkedList<>();
        SqlNodeList sqlNodes = MyblinkSqlUtils.parseSql(tableSql);
        for(SqlNode sqlNode :sqlNodes){
            if(sqlNode instanceof SqlCreateTable){
                tableList.add((SqlCreateTable) sqlNode);

                List<SqlTableColumn> tableColumns = new ArrayList<>();
                List<SqlBasicCall> basicCalls = new ArrayList<>();
                Map<String,String> tableColumnInfo = new LinkedHashMap<>();
                List<String> basicCallInfo = new ArrayList<>();
                SqlNodeList columnList = ((SqlCreateTable) sqlNode).getColumnList();
                for(SqlNode sqlNode1:columnList){
                    if(sqlNode1 instanceof SqlTableColumn){
                        tableColumns.add((SqlTableColumn) sqlNode1);
                        SqlIdentifier columnName = ((SqlTableColumn) sqlNode1).getName();
                        SqlDataTypeSpec type = ((SqlTableColumn) sqlNode1).getType();
                        tableColumnInfo.put(columnName.toString(),type.toString());
                    }
                    if(sqlNode1 instanceof SqlBasicCall){
                        basicCalls.add((SqlBasicCall) sqlNode1);
                        basicCallInfo.add(sqlNode1.toString());
                    }
                }

            }
        }
        return tableList;
    }

    @Override
    public List<RichSqlInsert> sqlInsertParser(String insertSql) throws Exception {
        List<RichSqlInsert> insertList = new LinkedList<>();
        SqlNodeList sqlNodes = MyblinkSqlUtils.parseSql(insertSql);
        for(SqlNode sqlNode:sqlNodes){
            if(sqlNode instanceof RichSqlInsert){
                insertList.add((RichSqlInsert) sqlNode);
            }
        }
        return insertList;
    }
}
