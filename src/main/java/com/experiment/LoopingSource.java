package com.experiment; // 请根据您的实际包名修改

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.TimeUnit;

/**
 * 自定义 Flink Source，通过循环读取一个 CSV 文件来模拟无限数据流。
 * 增加了对空行和解析错误的健壮性处理。
 */
public class LoopingSource implements SourceFunction<TaxiRide> {

    private volatile boolean isRunning = true;
    private final String dataPath;

    // 模拟流速的延迟（毫秒）
    private static final long SLEEP_MS_PER_RECORD = 1;

    public LoopingSource(String dataPath) {
        this.dataPath = dataPath;
    }

    @Override
    public void run(SourceContext<TaxiRide> ctx) throws Exception {

        // 外部无限循环：保证文件读取完成后能重新开始
        while (isRunning) {

            System.out.println("LoopingSource: Starting a new pass over the CSV file at " + dataPath);

            // 使用 try-with-resources 确保 BufferedReader 资源在每次循环中都被正确关闭和重新创建
            try (BufferedReader reader = new BufferedReader(new FileReader(dataPath))) {

                // 跳过 CSV header 行
                reader.readLine();

                String line;
                // 内部循环：读取文件直到结束
                while (isRunning && (line = reader.readLine()) != null) {

                    // 💥 1. 检查并跳过空行或只包含空格的行
                    if (line.trim().isEmpty()) {
                        System.out.println("Skipping empty line.");
                        continue;
                    }

                    try {
                        // 2. 解析数据
                        // 如果数据中含有缺失值，TaxiRide.fromString(line) 可能会抛出异常
                        TaxiRide ride = TaxiRide.fromString(line);

                        // 3. 模拟 Event Time 行为：使用当前系统时间作为事件时间
                        long currentEventTime = System.currentTimeMillis();

                        // 4. 发送数据和水位线（需要 Checkpoint Lock 保证 Checkpoint 时的状态一致性）
                        synchronized (ctx.getCheckpointLock()) {
                            ctx.collectWithTimestamp(
                                new TaxiRide(
                                    currentEventTime,
                                    ride.puLocationId,
                                    ride.totalAmount
                                ),
                                currentEventTime
                            );
                            // 发射水位线
                            ctx.emitWatermark(new Watermark(currentEventTime - 1000));
                        }

                        // 5. 流控
                        TimeUnit.MILLISECONDS.sleep(SLEEP_MS_PER_RECORD);

                    } catch (Exception parseException) {
                        // 💥 捕获并跳过格式错误（例如缺失值导致的 NumberFormatException 等）的记录
                        System.err.println("Skipping bad record: " + line + ". Error: " + parseException.getMessage());
                        // 继续到下一行
                    }
                }

            } catch (Exception fileException) {
                // 如果文件访问出现问题（如被移动、权限错误），这里会捕获到异常
                System.err.println("Error accessing CSV file in LoopingSource: " + fileException.getMessage());
                // 失败后等待 5 秒重试
                TimeUnit.SECONDS.sleep(5);
            }

            System.out.println("LoopingSource: Finished pass. Restarting file read loop.");
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}