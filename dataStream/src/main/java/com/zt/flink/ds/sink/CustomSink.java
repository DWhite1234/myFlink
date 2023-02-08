package com.zt.flink.ds.sink;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zt
 * @{date}
 */

public class CustomSink extends RichSinkFunction<String> implements CheckpointedFunction, CheckpointListener {
    //从ck中恢复的数据
    private ListState<String> restoreListState;
    private List<String> list;

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化发送失败的数据存放list
        list = new ArrayList<>();
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        //具体的逻辑处理

        //如果发送失败,存入list中
        list.add(value);
    }



    /**
     * 初始化状态
     * @param context the context for initializing the operator
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        /*
        1.从ck中恢复sink的状态,只有两种方式
            1.listState: 从ck中恢复的数据会以轮询的方式发送给不同的sink并行度
            2.unionListState: 从ck中恢复的数据,每个sink并行度都拥有相同的一份
         */
        //判断是否是从ck中恢复
        if (context.isRestored()) {
            restoreListState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("listState", String.class));
        }
    }

    /**
     * ck开始时执行
     * @param context the context for drawing a snapshot of the operator
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        //将list中的数据准备写入listState中
        restoreListState.addAll(list);
    }

    /**
     * ck完成时执行
     * @param checkpointId The ID of the checkpoint that has been completed.
     * @throws Exception
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        /*
        应用场景1:把sink中listState的数据清除,因为ck已经成功写入
        应用场景2:可以做一些事务的提交
         */
        //清除上次ck中写入的残留数据
        restoreListState.clear();
    }
}
