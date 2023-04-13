package com.zt.flink.java.utils;

import com.alibaba.druid.pool.DruidDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.io.IOException;


@Slf4j
public class JDBCutil {
    public static DataSource initDataSource(ParameterTool config) {
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setUsername(config.get("jdbcUserName","root"));
        dataSource.setPassword(config.get("jdbcPassword","123456"));
        dataSource.setDriverClassName(config.get("jdbcDriverName","com.mysql.jdbc.Driver"));
        dataSource.setUrl(config.get("jdbcUrl","jdbc:mysql: "));
        //初始化连接数
        dataSource.setInitialSize(config.getInt("initSize",5));
        //最小空闲连接数
        dataSource.setMinIdle(config.getInt("minIdle",3));
        //最大允许连接数
        dataSource.setMaxActive(config.getInt("maxActive",10));
        //是否保持连接
        dataSource.setKeepAlive(config.getBoolean("isKeepAlive",false));
        //检测关闭连接的时间间隔
        dataSource.setTimeBetweenEvictionRunsMillis(config.getLong("timeBetweenEvictionRunMills",60000L));
        //连接最小生存时间
        dataSource.setMinEvictableIdleTimeMillis(config.getLong("minEvictableIdleTimeMills",60000L));
        //空闲是否检测连接可用性
        dataSource.setTestWhileIdle(config.getBoolean("testWhileIdle",false));
        //获取连接时,是否检测连接可用
        dataSource.setTestOnBorrow(config.getBoolean("testOnBorrow",true));
        //归还连接,检测连接可用
        dataSource.setTestOnReturn(config.getBoolean("testOnReturn",false));
        //检测连接是否可用sql语句
        dataSource.setValidationQuery(config.get("validateQuery","select 1"));
        //检测链接是否可用超时时间
        dataSource.setValidationQueryTimeout(config.getInt("validateQueryTimeOut",5000));
        //强制回收
        dataSource.removeAbandoned();
        log.info("初始化连接池参数:{}",dataSource);
        return dataSource;
    }

    public static TransactionTemplate getTransactionManager(DataSource dataSource) {
        DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(dataSource);
        return new TransactionTemplate(transactionManager);
    }

    //使用
    public static void main(String[] args) throws IOException {
        //创建jdbcTemplate
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        DataSource dataSource = initDataSource(parameterTool);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        //创建transactionTemplate
        TransactionTemplate transactionTemplate = getTransactionManager(dataSource);
        //进行事务控制
        transactionTemplate.execute(new TransactionCallback<Object>() {
            @Override
            public Object doInTransaction(TransactionStatus transactionStatus) {

                try {
                    //执行sql
                    int rows = jdbcTemplate.update("");
                    return null;
                } catch (Exception e) {
                    //事务回滚
                    transactionStatus.setRollbackOnly();
                }
                return null;
            }
        });
    }
}
