package gientech.client.sql;

import java.util.List;
import java.util.Map;

/**
 * author: yhl
 * time: 2021/6/18 下午4:57
 * company: gientech
 */
public interface SqlConvertService {
    Map<String, List<String>> sqlConvert(String sqlContext) throws Exception;
}
