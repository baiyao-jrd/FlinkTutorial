package org.baiyao.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .socketTextStream("hadoop102", 8888)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    public void flatMap(String in, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] words = in.split(" ");
                        for (String word : words) {
                            out.collect(Tuple2.of(word, 1L));
                        }
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    public String getKey(Tuple2<String, Long> in) throws Exception {
                        return in.f0;
                    }
                })
                .sum(1)
                .print();
        env.execute();
    }
}
