package com.kafka.offset;

import com.sun.xml.internal.bind.v2.runtime.Coordinator;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.runtime.checkpoint.*;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.util.Runnables;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.TreeMap;
import java.util.concurrent.Executor;

/**
 * @author zt
 */
@Slf4j
public class MyKafkaSource extends RichParallelSourceFunction<String> implements CheckpointedFunction{
    private TreeMap<KafkaTopicPartition, Long> restoredState;
    private ListState<Tuple2<KafkaTopicPartition, Long>> unionOffsetStates;

    @Override
    public void open(Configuration parameters) throws Exception {
    }

    @Override
    public void run(SourceFunction.SourceContext<String> ctx) throws Exception {
    }

    @Override
    public void cancel() {

    }

    /**
     * 执行ck时调用
     * @param context the context for drawing a snapshot of the operator
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    /**
     * 从ck中恢复
     * @param context the context for initializing the operator
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        OperatorStateStore store = context.getOperatorStateStore();
        //获取所有并行度的ck状态
        unionOffsetStates = store.getUnionListState(new ListStateDescriptor<>("kafka", TypeInformation.of(new TypeHint<Tuple2<KafkaTopicPartition, Long>>() {
        })));
        if (context.isRestored()) {
            //排序
            restoredState = new TreeMap<>(new KafkaTopicPartition.Comparator());
            //归还分区
            for (Tuple2<KafkaTopicPartition, Long> topicsPartitions : unionOffsetStates.get()) {
                restoredState.put(topicsPartitions.f0, topicsPartitions.f1);
                log.info("Consumer subtask {} restored state: {}",getRuntimeContext().getIndexOfThisSubtask(),restoredState);
            }
        }else{
            log.info("Consumer subtask {} has no state to restore",getRuntimeContext().getIndexOfThisSubtask());
        }
    }

}
