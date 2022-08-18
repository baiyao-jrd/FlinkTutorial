package org.baiyao.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        //1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2. 统一设置算子的并行子任务数量为1
        env.setParallelism(1);
        env
                //3. 按行读取文件数据
                .readTextFile("input/words.txt")
                //4. 数据格式转换
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    public void flatMap(String in, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] words = in.split(" ");
                        for (String word : words) {
                            out.collect(Tuple2.of(word, 1L));
                        }
                    }
                })
                //5. 按照元组key进行分组
                .groupBy(0)
                //6. 分组内聚合统计
                .sum(1)
                //7. 打印结果
                .print();
    }
}
