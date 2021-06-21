package gientech.common.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * author: yhl
 * time: 2021/6/18 下午4:36
 * company: gientech
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class JobParameter {
    private String jobName;
    private Map<String, List<String>> sqls;


}
