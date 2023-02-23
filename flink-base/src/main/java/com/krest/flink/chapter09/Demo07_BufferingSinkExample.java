package com.krest.flink.chapter09;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial
 * <p>
 * Created by  wushengran
 */


import com.krest.flink.chapter05.ClickSource;
import com.krest.flink.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * 算子状态
 */
public class Demo07_BufferingSinkExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 开启检查点，每隔10s做检查点， 默认是 500 毫秒
        env.enableCheckpointing(10000L);
        // 可以为每个任务单独去设置检查点
        // env.setStateBackend(new EmbeddedRocksDBStateBackend());
        // 设置检查点的存储位置
        // env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(""));

        // 检查点环境设置
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 检查点生成策略：精确一次 、 至少一次（涉及状态一致性）
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 与上一个检查点之间的间隔至少有 500 ms
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        // 超过多长时间，检查点会被丢弃
        checkpointConfig.setCheckpointTimeout(60000);
        // 最多正在处理的检查点数量
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // 开启外部持久化存储: 作业取消的时候也会保留外部检查点。
        checkpointConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 不对齐检查点
        checkpointConfig.enableUnalignedCheckpoints();


        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        stream.print("input");

        // 批量缓存输出(10条数据，然后一起输出)
        stream.addSink(new BufferingSink(10));

        env.execute();
    }

    /**
     * 批量输出接口
     */
    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {

        private final int threshold;
        private transient ListState<Event> checkpointedState;
        private List<Event> bufferedElements;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        /**
         * 输出功能方法
         *
         * @param value
         * @param context
         * @throws Exception
         */
        @Override
        public void invoke(Event value, Context context) throws Exception {
            bufferedElements.add(value);
            // 如果缓存达到阈值，就直接输出打印
            if (bufferedElements.size() == threshold) {
                for (Event element : bufferedElements) {
                    // 输出到外部系统，这里用控制台打印模拟
                    System.out.println(element);
                }
                System.out.println("==========输出完毕=========");
                bufferedElements.clear();
            }
        }

        /**
         * 持久点功能接口
         *
         * @param context
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // 清空已有的数据
            checkpointedState.clear();
            // 把当前局部变量中的所有元素写入到检查点中
            for (Event element : bufferedElements) {
                checkpointedState.add(element);
            }
        }

        /**
         * 初始化算子状态
         *
         * @param context
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 真正的定义算子状态
            ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>(
                    "buffered-elements",
                    Types.POJO(Event.class));

            // 获取算子状态
            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

            // 如果是从故障中恢复，就将ListState中的所有元素添加到局部变量中（故障数据恢复）
            if (context.isRestored()) {
                for (Event element : checkpointedState.get()) {
                    bufferedElements.add(element);
                }
            }
        }
    }
}
