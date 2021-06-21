package gientech.client.job;

import gientech.common.entity.JobParameter;
import org.apache.flink.api.common.Plan;
import org.apache.flink.runtime.jobgraph.JobGraph;

import java.util.Map;

/**
 * author: yhl
 * time: 2021/6/18 下午4:31
 * company: gientech
 */
public interface JobClient {
    Plan getJobPlan(JobParameter jobParameter, Map<String,String> extParams) throws Exception;
    JobGraph getJobGraph(JobParameter jobParameter,Map<String,String> extParams) throws Exception;
}
