package bbejeck.chapter_7.interceptors;


import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ZMartProducerInterceptor implements ProducerInterceptor<Object, Object> {


    private static final Logger LOG = LoggerFactory.getLogger(ZMartProducerInterceptor.class);

    /*
        프로듀서에서 레코드를 브로커에 전송하기 바로 전에 레코드를 로깅할 수 있다.
     */
    @Override
    public ProducerRecord<Object, Object> onSend(ProducerRecord<Object, Object> record) {
        LOG.info("ProducerRecord being sent out {} ", record);
        return record;
    }

    /*
        브로커 수신 확인(acknowledgement) 또는 생산 단계 동안 브로커 측에서 오류가 발생했는지 로깅할 수 있다.
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            LOG.warn("Exception encountered producing record {}", exception);
        } else {
            LOG.info("record has been acknowledged {} ", metadata);
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
