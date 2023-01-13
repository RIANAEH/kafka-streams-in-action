package bbejeck.chapter_8;

import bbejeck.model.Purchase;
import bbejeck.model.PurchasePattern;
import bbejeck.model.RewardAccumulator;
import bbejeck.util.datagen.DataGenerator;
import bbejeck.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.test.ProcessorTopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * User: Bill Bejeck
 * Date: 9/9/17
 * Time: 2:39 PM
 */
public class ZMartTopologyTest {

    private  ProcessorTopologyTestDriver topologyTestDriver;

    @BeforeEach
    public  void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "FirstZmart-Kafka-Streams-Client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "zmart-purchases");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "FirstZmart-Kafka-Streams-App");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

        StreamsConfig streamsConfig = new StreamsConfig(props);
        Topology topology = ZMartTopology.build();
        
        topologyTestDriver = new ProcessorTopologyTestDriver(streamsConfig, topology);
    }

    @Test
    @DisplayName("Testing the ZMart Topology Flow")
    public void testZMartTopology() {

        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
        Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();
        Serde<String> stringSerde = Serdes.String();

        Purchase purchase = DataGenerator.generatePurchase();

        // 토폴로지에 초기 레코드 전송
        topologyTestDriver.process("transactions",
                null,
                purchase,
                stringSerde.serializer(),
                purchaseSerde.serializer());

        // 1. purchases 토픽에서 레코드 조회
        ProducerRecord<String, Purchase> record = topologyTestDriver.readOutput("purchases",
                stringSerde.deserializer(),
                purchaseSerde.deserializer());

        // 잘 마스킹 되어 있는지 확인
        Purchase expectedPurchase = Purchase.builder(purchase).maskCreditCard().build();
        assertThat(record.value(), equalTo(expectedPurchase));

        // 2. rewards 토픽에서 레코드 조회
        ProducerRecord<String, RewardAccumulator> accumulatorProducerRecord = topologyTestDriver.readOutput("rewards",
                stringSerde.deserializer(),
                rewardAccumulatorSerde.deserializer());

        // 보상이 잘 계산되었는지 확인
        RewardAccumulator expectedRewardAccumulator = RewardAccumulator.builder(expectedPurchase).build();
        assertThat(accumulatorProducerRecord.value(), equalTo(expectedRewardAccumulator));

        // 3. patterns 토픽에서 레코드 조회
        ProducerRecord<String, PurchasePattern> purchasePatternProducerRecord = topologyTestDriver.readOutput("patterns",
                stringSerde.deserializer(),
                purchasePatternSerde.deserializer());

        // 구매 패턴이 잘 조회되었는지 확인
        PurchasePattern expectedPurchasePattern = PurchasePattern.builder(expectedPurchase).build();
        assertThat(purchasePatternProducerRecord.value(), equalTo(expectedPurchasePattern));
    }
}
