package gientech.stream.impl;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import gientech.client.job.JobClient;
import gientech.client.sql.SqlParserService;
import gientech.common.entity.JobParameter;
import gientech.common.exception.FlinkSqlException;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.Plan;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import sql.SqlConstant;
import sql.impl.SqlParserServiceImpl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * author: yhl
 * time: 2021/6/18 下午5:38
 * company: gientech
 */
@Slf4j
public class StreamJobClinetImpl implements JobClient {
    private SqlParserService sqlParserService = new SqlParserServiceImpl();

    @Override
    public Plan getJobPlan(JobParameter jobParameter, Map<String, String> extParams) throws Exception {
        return null;
    }

    @Override
    public JobGraph getJobGraph(JobParameter jobParameter, Map<String, String> extParams) throws Exception {
        return null;
    }

    private StreamExecutionEnvironment getStreamLocalExecution(JobParameter jobParameter,Map<String,String> extParams) throws Exception{
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        Map<String, List<String>> sqls = jobParameter.getSqls();

        //function
        if(sqls.containsKey(SqlConstant.FUNCTION)){
            List<String> funList = sqls.get(SqlConstant.FUNCTION);
            if(CollectionUtils.isNotEmpty(funList)){
                for(String sql:funList){
                    List<SqlCreateFunction> functions = sqlParserService.sqlFunctionParser(sql);
                    for(SqlCreateFunction fun:functions){
                            tableEnv.sqlUpdate(fun.toString());
                    }
                }
            }
        }

        // view
        if (sqls.containsKey(SqlConstant.VIEW)) {
            List<String> viewList = sqls.get(SqlConstant.VIEW);
            if (CollectionUtils.isNotEmpty(viewList)) {
                for (String sql : viewList) {
                    List<SqlCreateView> views = sqlParserService.sqlViewParser(sql);
                    if (CollectionUtils.isNotEmpty(views)) {
                        for (SqlCreateView view : views) {
                            Table table = tableEnv.sqlQuery(view.getQuery().toString());
                            tableEnv.createTemporaryView(view.getViewName().toString(), table);
                        }
                    }
                }
            }
        }
        //insert into
        List<String> insertList = sqls.get(SqlConstant.INSERT);
        if(CollectionUtils.isEmpty(insertList)){
            log.warn("insert sql not null");
            throw new FlinkSqlException("insert sql not null");
        }
        Map<String, RichSqlInsert> dmlSqlNodes = new HashMap<>();
        for(String sql:insertList){
            List<RichSqlInsert> richSqlInserts = sqlParserService.sqlInsertParser(sql);
            if(CollectionUtils.isNotEmpty(richSqlInserts)){
                for(RichSqlInsert richSqlInsert:richSqlInserts){
                    SqlNode table = richSqlInsert.getTargetTable();
                    dmlSqlNodes.put(table.toString(),richSqlInsert);
                }
            }
        }

        //table
        List<String> tableList = sqls.get(SqlConstant.TABLE);
        if(CollectionUtils.isEmpty(tableList)){
            log.warn("table sql not null");
            throw new FlinkSqlException("table sql not null");
        }
        Map<String, SqlCreateTable> tableSqlNodes = new HashMap<>();
        for(String sql:tableList){
            List<SqlCreateTable> sqlCreateTables = sqlParserService.sqlTableParser(sql);
            if(CollectionUtils.isNotEmpty(sqlCreateTables)){
                for(SqlCreateTable sqlCreateTable:sqlCreateTables){
                    SqlIdentifier table = sqlCreateTable.getTableName();
                    tableSqlNodes.put(table.toString(),sqlCreateTable);
                }
            }
        }
        //通过insert into 语句获得source  sink 的tablen信息

        //获得source
        MapDifference<String, ? extends SqlCall> difference = Maps.difference(dmlSqlNodes, tableSqlNodes);
        Map<String, ? extends SqlCall> sourceMap = difference.entriesOnlyOnRight();
        //获得sink
        MapDifference<String, SqlCall> difference1 = Maps.difference(sourceMap, tableSqlNodes);
        Map<String, SqlCall> sinkMap = difference1.entriesOnlyOnRight();

        //处理source
        for(String tablename:sourceMap.keySet()){
            SqlCall sqlCall = sourceMap.get(tablename);
            if(sqlCall instanceof SqlCreateTable){
                tableEnv.sqlUpdate(sqlCall.toString());
            }
        }
        //处理sink
        for(String tablename:sinkMap.keySet()){
            SqlCall sqlCall = sinkMap.get(tablename);
            if(sqlCall instanceof SqlCreateTable){
                tableEnv.sqlUpdate(sqlCall.toString());
            }
        }

        return env;
    }
}
