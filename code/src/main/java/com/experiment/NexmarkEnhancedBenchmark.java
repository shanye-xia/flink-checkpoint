package com.experiment;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * 实验专用 Nexmark 增强版 Benchmark
 * 修改点：增加 MetricSink 用于 Python 脚本采集延迟指标
 */
public class NexmarkEnhancedBenchmark {

    public static void main(String[] args) throws Exception {

        // 1. 获取运行参数
        ParameterTool params = ParameterTool.fromArgs(args);
        long checkpointInterval = params.getLong("interval", 10000); 
        int parallelism = params.getInt("parallelism", 2);           
        int rate = params.getInt("rate", 15000);                     
        int maxAuctionId = params.getInt("keys", 200000);            
        String stateMode = params.get("stateMode", "hybrid");        

        // 2. Flink 环境配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        // Checkpoint 设置
        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 建议：实验时设置 Checkpoint 超时，防止卡死
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        // 3. 数据源生成
        DataGeneratorSource<Tuple4<Long, Long, Long, Long>> generatorSource =
                new DataGeneratorSource<>(
                        new GeneratorFunction<Long, Tuple4<Long, Long, Long, Long>>() {
                            private final Random random = new Random();
                            @Override
                            public Tuple4<Long, Long, Long, Long> map(Long value) {
                                long auctionId = random.nextInt(maxAuctionId); 
                                long bidderId = random.nextInt(3_000_000);
                                long price = random.nextInt(5000) + 1;
                                // f3 是事件生成时间
                                return Tuple4.of(auctionId, bidderId, price, System.currentTimeMillis());
                            }
                        },
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(rate),
                        Types.TUPLE(Types.LONG, Types.LONG, Types.LONG, Types.LONG)
                );

        DataStream<Tuple4<Long, Long, Long, Long>> rawStream = env.fromSource(
                generatorSource,
                WatermarkStrategy.noWatermarks(),
                "Nexmark-Bid-Source"
        );

        // 4. 大状态模拟 (制造 Checkpoint 压力)
        DataStream<Tuple4<Long, Long, Long, Long>> enriched =
                rawStream
                        .keyBy(t -> t.f1) // By BidderID
                        .process(new LargeStateMapper(stateMode))
                        .name("State-Enrichment");

        // 5. 窗口统计与 Sink (输出到 MetricSink)
        enriched
                .keyBy(t -> t.f0) // By AuctionID
                // 使用处理时间窗口 (10s 窗口, 2s 滑动)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .aggregate(new CountAggregator(), new WindowResult())
                .name("Window-Aggregation")
                // 使用自定义 Sink 暴露延迟指标
                .addSink(new MetricSink())
                .name("Latency-Metric-Sink");

        env.execute("Nexmark Bench (Interval: " + checkpointInterval + "ms)");
    }

    // ============================================================
    //  A. 状态模拟 (保持不变)
    // ============================================================
    private static class LargeStateMapper
            extends KeyedProcessFunction<Long, Tuple4<Long, Long, Long, Long>, Tuple4<Long, Long, Long, Long>> {

        private final String mode;
        private transient ValueState<byte[]> profileState;
        private final Random random = new Random();

        public LargeStateMapper(String mode) { this.mode = mode; }

        @Override
        public void open(Configuration parameters) {
            profileState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("profile", TypeInformation.of(new TypeHint<byte[]>() {})));
        }

        @Override
        public void processElement(Tuple4<Long, Long, Long, Long> value, Context ctx, Collector<Tuple4<Long, Long, Long, Long>> out) throws Exception {
            byte[] state = profileState.value();
            if (state == null) state = allocateBytesByMode();
            int idx = random.nextInt(state.length);
            state[idx] = (byte) (state[idx] + 1);
            profileState.update(state);
            out.collect(value);
        }

        private byte[] allocateBytesByMode() {
            // 简单处理：hybrid 模式
            if ("bigstate".equals(mode)) return new byte[20000 + random.nextInt(180000)];
            if ("realistic".equals(mode)) return new byte[500 + random.nextInt(1500)];
            return (random.nextDouble() < 0.8) ? new byte[1000] : new byte[50000];
        }
    }

    // ============================================================
    //  B. 窗口相关
    // ============================================================

    /** 简单计数器 */
    private static class CountAggregator implements AggregateFunction<Tuple4<Long, Long, Long, Long>, Long, Long> {
        @Override public Long createAccumulator() { return 0L; }
        @Override public Long add(Tuple4<Long, Long, Long, Long> value, Long acc) { return acc + 1; }
        @Override public Long getResult(Long acc) { return acc; }
        @Override public Long merge(Long a, Long b) { return a + b; }
    }

    /** 
     * 修改后的 WindowResult
     * 输出: Tuple3<AuctionID, Count, WindowEndTime> 
     * 我们传递 WindowEndTime 是为了在 Sink 计算延迟
     */
    private static class WindowResult implements WindowFunction<Long, Tuple3<Long, Long, Long>, Long, TimeWindow> {
        @Override
        public void apply(Long key, TimeWindow window, Iterable<Long> input, Collector<Tuple3<Long, Long, Long>> out) {
            Long count = input.iterator().next();
            // 只输出热点数据以减少 Sink 压力，或者输出全部以测试吞吐
            if (count > 0) { 
                // f0: key, f1: count, f2: windowEndTime
                out.collect(Tuple3.of(key, count, window.getEnd()));
            }
        }
    }

    // ============================================================
    //  C. [关键] 自定义 Sink 用于暴露 Metric
    // ============================================================
    
    /**
     * MetricSink
     * 作用：接收处理结果，计算延迟，并通过 Flink Metric System 暴露给 Python 脚本
     */
    public static class MetricSink extends RichSinkFunction<Tuple3<Long, Long, Long>> {
        private transient volatile long currentLatency = 0;

        @Override
        public void open(Configuration parameters) {
            // 注册名为 "e2e_latency_ms" 的 Gauge
            // Python 脚本可以通过 REST API 读取这个值
            getRuntimeContext()
                .getMetricGroup()
                .gauge("e2e_latency_ms", (Gauge<Long>) () -> currentLatency);
        }

        @Override
        public void invoke(Tuple3<Long, Long, Long> value, Context context) {
            long windowEndTime = value.f2;
            long now = System.currentTimeMillis();
            
            // 计算延迟：当前时间 - 窗口本该结束的时间
            // 如果系统处理慢，now 会远大于 windowEndTime，延迟就高
            long latency = now - windowEndTime;
            
            // 避免负数（虽然 ProcessingTime 下不太可能）
            this.currentLatency = Math.max(0, latency);
        }
    }
}