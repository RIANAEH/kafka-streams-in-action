package bbejeck.chapter_6;


import bbejeck.chapter_6.processor.KStreamPrinter;
import bbejeck.chapter_6.processor.cogrouping.ClickEventProcessor;
import bbejeck.chapter_6.processor.cogrouping.CogroupingProcessor;
import bbejeck.chapter_6.processor.cogrouping.StockTransactionProcessor;
import bbejeck.clients.producer.MockDataProducer;
import bbejeck.model.ClickEvent;
import bbejeck.model.StockTransaction;
import bbejeck.util.collection.Tuple;
import bbejeck.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static bbejeck.chapter_6.processor.cogrouping.CogroupingProcessor.TUPLE_STORE_NAME;

public class CoGroupingApplication {

    public static void main(String[] args) throws Exception {

        StreamsConfig streamsConfig = new StreamsConfig(getProperties());
        Deserializer<String> stringDeserializer = Serdes.String().deserializer();
        Serializer<String> stringSerializer = Serdes.String().serializer();
        Serde<Tuple<List<ClickEvent>, List<StockTransaction>>> eventPerformanceTuple = StreamsSerdes.EventTransactionTupleSerde();
        Serializer<Tuple<List<ClickEvent>, List<StockTransaction>>> tupleSerializer = eventPerformanceTuple.serializer();
        Serde<StockTransaction> stockTransactionSerde = StreamsSerdes.StockTransactionSerde();
        Deserializer<StockTransaction> stockTransactionDeserializer = stockTransactionSerde.deserializer();

        Serde<ClickEvent> clickEventSerde = StreamsSerdes.ClickEventSerde();
        Deserializer<ClickEvent> clickEventDeserializer = clickEventSerde.deserializer();

        // 토폴로지 생성
        Topology topology = new Topology();

        // 주식 거래 내역 토픽의 소스 노드 및 프로세서 추가
        topology.addSource("Txn-Source",
                stringDeserializer,
                stockTransactionDeserializer,
                "stock-transactions");
        topology.addProcessor("Txn-Processor",
                StockTransactionProcessor::new,
                "Txn-Source");

        // 이벤트 토픽의 소스 노드 및 프로세서 추가
        topology.addSource("Events-Source",
                stringDeserializer,
                clickEventDeserializer,
                "events");
        topology.addProcessor("Events-Processor",
                ClickEventProcessor::new,
                "Events-Source");

        // 코그룹 프로세서 추가
        topology.addProcessor("CoGrouping-Processor",
                CogroupingProcessor::new,
                "Txn-Processor", "Events-Processor"); // 두개의 부모 노드
        // 코그룹 프로세서가 사용하는 스테이트 스토어 추가
        topology.addStateStore(makePersistentStoreBuilder(eventPerformanceTuple),
                "CoGrouping-Processor"); // 스테이트 스토어를 사용하는 프로세서 (여러개 지정 가능)

        // 결과를 저장하는 싱크 노드 추가
//        topology.addSink("Tuple-Sink",
//                "cogrouped-results",
//                stringSerializer,
//                tupleSerializer,
//                "CoGrouping-Processor");

        topology.addProcessor("Print",
                new KStreamPrinter("Co-Grouping"),
                "CoGrouping-Processor");


        MockDataProducer.produceStockTransactionsAndDayTradingClickEvents(50, 100, 100, StockTransaction::getSymbol);

        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsConfig);
        System.out.println("Co-Grouping App Started");
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Thread.sleep(70000);
        System.out.println("Shutting down the Co-Grouping App now");
        kafkaStreams.close();
        MockDataProducer.shutdown();
    }

    private static StoreBuilder<KeyValueStore<String, Tuple<List<ClickEvent>, List<StockTransaction>>>> makePersistentStoreBuilder(final Serde<Tuple<List<ClickEvent>, List<StockTransaction>>> eventPerformanceTuple) {
        // 스테이트 스토어의 압축 및 삭제 기간 설정
        Map<String, String> changeLogConfigs = new HashMap<>();
        changeLogConfigs.put("retention.ms", "120000");
        changeLogConfigs.put("cleanup.policy", "compact,delete");

        // 영구 저장소(록스DB)를 위한 저장소 서플라이어 생성
        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(TUPLE_STORE_NAME);

        // 저장소 빌더 생성
        return Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), eventPerformanceTuple)
                .withLoggingEnabled(changeLogConfigs); // 저장소 빌더에 변경로그 구성을 추가
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "cogrouping-client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "cogrouping-group");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "cogrouping-appid");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }
}
