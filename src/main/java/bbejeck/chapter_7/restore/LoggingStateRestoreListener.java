package bbejeck.chapter_7.restore;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: Bill Bejeck
 * Date: 8/19/17
 * Time: 7:54 PM
 */
public class LoggingStateRestoreListener implements StateRestoreListener {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingStateRestoreListener.class);
    private final Map<TopicPartition, Long> totalToRestore = new ConcurrentHashMap<>();
    private final Map<TopicPartition, Long> restoredSoFar = new ConcurrentHashMap<>();


    /*
        복원 프로세스가 시작될 때 실행한다.
     */
    @Override
    public void onRestoreStart(TopicPartition topicPartition, String store, long start, long end) {
        long toRestore = end - start;
        totalToRestore.put(topicPartition, toRestore);
        LOG.info("Starting restoration for {} on topic-partition {} total to restore {}", store, topicPartition, toRestore);

    }

    /*
        복원 프로세스가 최근 배치를 상태 저장소에 로드한 후에 실행한다.
     */
    @Override
    public void onBatchRestored(TopicPartition topicPartition, String store, long start, long batchCompleted) {
        NumberFormat formatter = new DecimalFormat("#.##");

        // 복원된 전체 레코드 개수 계산
        long currentProgress = batchCompleted + restoredSoFar.getOrDefault(topicPartition, 0L);
        // 완료된 복원의 백분율 계산
        double percentComplete =  (double) currentProgress / totalToRestore.get(topicPartition);

        LOG.info("Completed {} for {}% of total restoration for {} on {}",
                batchCompleted, formatter.format(percentComplete * 100.00), store, topicPartition);

        // 지금까지 복원된 레코드 개수 저장
        restoredSoFar.put(topicPartition, currentProgress);
    }

    /*
        복원 프로세스가 모두 완료되면 실행한다.
     */
    @Override
    public void onRestoreEnd(TopicPartition topicPartition, String store, long totalRestored) {
        LOG.info("Restoration completed for {} on topic-partition {}", store, topicPartition);
        restoredSoFar.put(topicPartition, 0L);
    }
}
