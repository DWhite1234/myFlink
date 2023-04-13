package com.zt.flink.ds.sink;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;


/**
 * @author zt
 */

public class TwoStageKafkaSink extends TwoPhaseCommitSinkFunction<String, KafkaProducer<String,String>,Void> {

    public TwoStageKafkaSink(TypeSerializer<KafkaProducer<String, String>> transactionSerializer, TypeSerializer<Void> contextSerializer) {
        super(transactionSerializer, contextSerializer);
    }


    @Override
    protected void invoke(KafkaProducer<String,String> transaction, String value, Context context) throws Exception {
        //数据来了就写入kafka
    }

    @Override
    protected KafkaProducer<String,String> beginTransaction() throws Exception {
        //1.初始化事务
        //2.开始事务
        return null;
    }

    @Override
    protected void preCommit(KafkaProducer<String,String> transaction) throws Exception {
        //不做处理
    }

    @Override
    protected void commit(KafkaProducer<String,String> transaction) {
        //1.提交事务
        //如果提交事务失败,则进入回收流程
            //2.事务提交失败将当前事务id放入队列中
            //3.并且将数据刷写,关闭生产者
    }

    @Override
    protected void abort(KafkaProducer<String,String> transaction) {
        //1.抛弃事务
        //如果提交事务失败,则进入回收流程
            //2.事务提交失败将当前事务id放入队列中
            //3.并且将数据刷写,关闭生产者
    }
}
