package com.krest.flink.chapter08;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial
 * <p>
 * Created by  wushengran
 */

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * 使用上面会比union的频率更加频繁，流中的数据类型可以不同
 * 两条流 connect 之后，数据类型会进行一次统一
 */
public class Demo04_ConnectTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Integer> stream1 = env.fromElements(1, 2, 3);
        DataStream<Long> stream2 = env.fromElements(1L, 2L, 3L);

        ConnectedStreams<Integer, Long> connectedStreams = stream1.connect(stream2);

        SingleOutputStreamOperator<String> result = connectedStreams
                // 参数一：流1 的数据类型，参数二： 流2 的数据类型，参数三： 最终返回的数据类型
                .map(new CoMapFunction<Integer, Long, String>() {
                    @Override
                    public String map1(Integer value) {
                        return "Integer: " + value;
                    }

                    @Override
                    public String map2(Long value) {
                        return "Long: " + value;
                    }
                });

        result.print();

        env.execute();
    }
}

