package gientech.common.exception;

/**
 * author: yhl
 * time: 2021/6/18 下午4:41
 * company: gientech
 */
public class FlinkSqlException extends Exception {
    public FlinkSqlException(){
        super();
    }
    public FlinkSqlException(String message){
        super(message);
    }
}
