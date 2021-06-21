package sql.impl;

import gientech.client.sql.SqlConvertService;
import gientech.common.MyBlinkStringUtils;
import gientech.common.exception.FlinkSqlException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import sql.SqlConstant;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * author: yhl
 * time: 2021/6/18 下午5:14
 * company: gientech
 */
@Slf4j
public class SqlConvertServiceImpl implements SqlConvertService {
    @Override
    public Map<String, List<String>> sqlConvert(String sqlContext) throws Exception {
        if(StringUtils.isBlank(sqlContext)){
            log.warn("sql is null");
            throw new FlinkSqlException("sql is null");
        }
        List<String> sqls = MyBlinkStringUtils.splitSemiColon(sqlContext);
        if(CollectionUtils.isEmpty(sqls)){
            log.warn("sqls is null");
            throw new FlinkSqlException("sqls is null");
        }
        Map<String,List<String>> result = new HashMap<>();
        //sql的顺序保持一致
        List<String> funcList = new LinkedList<>();
        List<String> tableList = new LinkedList<>();
        List<String> viewList = new LinkedList<>();
        List<String> insertList = new LinkedList<>();

        for(String sql:sqls){
            if(StringUtils.isNoneBlank(sql)){
                if(MyBlinkStringUtils.isContain(SqlConstant.CREATE_FUNCTION,sql)){
                    funcList.add(sql);
                }
                if(MyBlinkStringUtils.isContain(SqlConstant.CREATE_TABLE,sql)){
                    tableList.add(sql);
                }
                if(MyBlinkStringUtils.isContain(SqlConstant.CREATE_VIEW,sql)){
                    viewList.add(sql);
                }
                if(MyBlinkStringUtils.isContain(SqlConstant.INSERT_INTO,sql)){
                    insertList.add(sql);
                }
            }
        }
        if(CollectionUtils.isEmpty(tableList)){
            log.warn("tableList is null");
            throw new FlinkSqlException("tableList is null");
        }
        result.put(SqlConstant.TABLE,tableList);
        if(CollectionUtils.isEmpty(insertList)){
            log.warn("insertList is null");
            throw new FlinkSqlException("insertList is null");
        }
        result.put(SqlConstant.INSERT,insertList);

        if(CollectionUtils.isNotEmpty(funcList)){
            result.put(SqlConstant.FUNCTION,funcList);
        }

        if(CollectionUtils.isNotEmpty(viewList)){
            result.put(SqlConstant.VIEW,viewList);
        }



        return result;
    }
}
