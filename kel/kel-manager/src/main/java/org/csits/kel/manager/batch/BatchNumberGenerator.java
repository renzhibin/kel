package org.csits.kel.manager.batch;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

/**
 * 批次号生成：yyyyMMddHHmmss_当日序号。
 * 当前节点内使用原子计数器，后续可替换为数据库序列。
 */
@Component
public class BatchNumberGenerator {

    private final AtomicInteger sequence = new AtomicInteger(0);

    private String currentDate = currentDatePrefix();

    public synchronized String nextBatchNumber() {
        String today = currentDatePrefix();
        if (!today.equals(currentDate)) {
            currentDate = today;
            sequence.set(0);
        }
        int seq = sequence.incrementAndGet();
        return today + "_" + String.format("%03d", seq);
    }

    private String currentDatePrefix() {
        return new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    }
}

