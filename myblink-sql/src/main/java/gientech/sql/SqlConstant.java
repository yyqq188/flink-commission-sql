package gientech.sql;

/**
 * author: yhl
 * time: 2021/6/18 下午5:07
 * company: gientech
 */
public interface SqlConstant {
    String CREATE_TABLE="(?i)^CREATE\\s+TABLE";
    String CREATE_TMP_FUNCTION="(?i)^CREATE\\s+TEMPORARY\\s+FUNCTION";
    String CREATE_FUNCTION="(?i)^CREATE\\s+FUNCTION";
    String CREATE_VIEW="(?i)^CREATE\\s+VIEW";
    String INSERT_INTO="(?i)^INSERT\\s+INTO";

    String FUNCTION="FUNCTION";
    String TABLE ="TABLE";
    String VIEW ="VIEW";
    String INSERT ="INSERT";
    String SQL_END_FLAG=";";
}
