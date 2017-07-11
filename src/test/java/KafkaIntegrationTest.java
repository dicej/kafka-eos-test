import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Iterator;
import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class KafkaIntegrationTest {
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(3, new Properties() {{
        put("auto.create.topics.enable", false);
    }});
    
    private final static String CONSUMER_GROUP_ID = "readCommitted";
    private static final String INPUT_TOPIC = "inputTopic";
    private static final String OUTPUT_TOPIC = "outputTopic";
    private static final String PROCESSOR_NAME = "statisticMachine";
    private static final String STORE_NAME = "mapStore";
    private static final int PARTITION_COUNT = 1;
    
    private static int testNumber = 0;
    
    @Before
    public void createTopics() throws Exception {
        CLUSTER.deleteTopicsAndWait( INPUT_TOPIC, OUTPUT_TOPIC );

        CLUSTER.createTopic(INPUT_TOPIC, PARTITION_COUNT, 1);
        CLUSTER.createTopic(OUTPUT_TOPIC, PARTITION_COUNT, 1);        
    }

    private static KafkaStreams makeStreams(Map<String, String> state) {
        TopologyBuilder builder = new TopologyBuilder()
            .addSource(INPUT_TOPIC, INPUT_TOPIC)
            .addStateStore(new MapStore.Supplier(STORE_NAME, state))       
            .addProcessor(PROCESSOR_NAME,
                          () -> new StatisticMachine(STORE_NAME),
                          INPUT_TOPIC)
            .connectProcessorAndStateStores(PROCESSOR_NAME, STORE_NAME);
        
        return new KafkaStreams
            (builder,
             StreamsTestUtils.getStreamsConfig
             ("appId-" + (++testNumber),
              CLUSTER.bootstrapServers(),
              Serdes.StringSerde.class.getName(),
              Serdes.StringSerde.class.getName(),
              new Properties() {{
                  put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
                      StreamsConfig.EXACTLY_ONCE);
              }}));
    }

    private static void produce(List<KeyValue<String, String>> input) throws Exception {
        IntegrationTestUtils.produceKeyValuesSynchronously
            (INPUT_TOPIC,
             input,
             TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                                      StringSerializer.class,
                                      StringSerializer.class),
             CLUSTER.time);
    }

    private static List<KeyValue<String, String>> consume(int count) throws Exception {
        return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived
            (TestUtils.consumerConfig
             (CLUSTER.bootstrapServers(),
              CONSUMER_GROUP_ID,
              StringDeserializer.class,
              StringDeserializer.class,
              new Properties() {{
                  put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                      IsolationLevel.READ_COMMITTED.name().toLowerCase());
              }}),
             OUTPUT_TOPIC,
             count);
    }

    @Test
    public void runEventMachine() throws Exception {
        {
            final Map<String, String> state = new TreeMap<>();

            KafkaStreams streams = makeStreams(state);
        
            try {
                streams.start();

                final List<KeyValue<String, String>> input
                    = new ArrayList<KeyValue<String, String>>() {{
                        add(new KeyValue<>("foo", "uno:one"));
                        add(new KeyValue<>("foo", "dos:two"));
                        add(new KeyValue<>("foo", "tres:three"));
                        add(new KeyValue<>("foo", "dos:,ni"));
                        add(new KeyValue<>("foo", "uno:,ichi"));
                    }};

                produce(input);
            
                final List<KeyValue<String, String>> expectedOutput
                    = new ArrayList<KeyValue<String, String>>() {{
                        add(new KeyValue<>("uno", "one"));
                        add(new KeyValue<>("dos", "two"));
                        add(new KeyValue<>("tres", "three"));
                        add(new KeyValue<>("dos", "two,ni"));
                        add(new KeyValue<>("uno", "one,ichi"));
                    }};

                final List<KeyValue<String, String>> output
                    = consume(expectedOutput.size());

                assertEquals(expectedOutput, output);

                final Map<String, String> expectedState
                    = new TreeMap<String, String>() {{
                        put("tres", "three");
                        put("dos", "two,ni");
                        put("uno", "one,ichi");                    
                    }};

                assertEquals(expectedState, state);
            } finally {
                streams.close();
            }
        }

        {
            final Map<String, String> state = new TreeMap<>();

            KafkaStreams streams = makeStreams(state);
        
            try {
                streams.start();

                final List<KeyValue<String, String>> input = new ArrayList<KeyValue<String, String>>() {{
                    add(new KeyValue<>("foo", "tres:,san"));
                    add(new KeyValue<>("foo", "dos:,2"));
                    add(new KeyValue<>("foo", "uno:,1"));
                    }};
            
                produce(input);
                    
                final List<KeyValue<String, String>> expectedOutput
                    = new ArrayList<KeyValue<String, String>>() {{
                        add(new KeyValue<>("uno", "one"));
                        add(new KeyValue<>("dos", "two"));
                        add(new KeyValue<>("tres", "three"));
                        add(new KeyValue<>("dos", "two,ni"));
                        add(new KeyValue<>("uno", "one,ichi"));
                        add(new KeyValue<>("tres", "three,san"));
                        add(new KeyValue<>("dos", "two,ni,2"));
                        add(new KeyValue<>("uno", "one,ichi,1"));
                    }};

                final List<KeyValue<String, String>> output
                    = consume(expectedOutput.size());

                assertEquals(expectedOutput, output);

                final Map<String, String> expectedState
                    = new TreeMap<String, String>() {{
                        put("tres", "three,san");
                        put("dos", "two,ni,2");
                        put("uno", "one,ichi,1");                    
                    }};

                assertEquals(expectedState, state);
            } finally {
                streams.close();
            }
        }
    }

    private static class StatisticMachine implements Processor<String, String> {
        private final String storeName;
        private KeyValueStore<String, String> store;

        public StatisticMachine(String storeName) {
            this.storeName = storeName;
        }
        
        @Override
        public void close() {
            // ignore
        }

        @Override
        public void init(ProcessorContext context) {
            this.store = (KeyValueStore<String, String>) context.getStateStore(storeName);
        }

        @Override
        public void process(String key, String value) {
            int index = value.indexOf(':');
            String newKey = value.substring(0, index);
            String newValue = value.substring(index + 1);
            String oldValue = store.get(newKey);
            store.put(newKey, (oldValue == null ? "" : oldValue) + newValue);
        }

        @Override
        public void punctuate(long timestamp) {
            // ignore
        }
    }

    private static class MapStore implements KeyValueStore<String, String> {
        private final String name;
        private final Map<String, String> map;
        private ProcessorContext context;
        private RecordCollector collector;
        private StateSerdes<String, String> serdes;
    
        public MapStore(String name, Map<String, String> map) {
            this.name = name;
            this.map = map;
        }
        
        @Override
        public String delete(String key) {
            return map.remove(key);
        }

        @Override
        public void put(String key, String value) {
            System.err.println("key " + key + " value " + value);
            
            map.put(key, value);

            collector.send(name(), key, value, context.taskId().partition,
                           context.timestamp(), serdes.keySerializer(),
                           serdes.valueSerializer());
        }

        @Override
        public void putAll(List<KeyValue<String, String>> entries) {
            for (KeyValue<String, String> entry : entries) {
                put(entry.key, entry.value);
            }
        }

        @Override
        public String putIfAbsent(String key, String value) {
            String current = map.get(key);
            if (current == null) {
                put(key, value);
                return null;
            } else {
                return current;
            }
        }

        @Override
        public KeyValueIterator<String, String> all() {
            return new KeyValueIterator<String, String>() {
                private Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
                
                @Override
                public void close() {
                    // ignore
                }

                @Override
                public String peekNextKey() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public KeyValue<String, String> next() {
                    Map.Entry<String, String> entry = it.next();
                    return new KeyValue<>(entry.getKey(), entry.getValue());
                }

                @Override
                public void remove() {
                    it.remove();
                }
            };
        }

        @Override
        public long approximateNumEntries() {
            return map.size();
        }

        @Override
        public String get(String key) {
            return map.get(key);
        }

        @Override
        public KeyValueIterator<String, String> range(String from, String to) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            // ignore
        }

        @Override
        public void flush() {
            // ignore
        }

        @Override
        public void init(ProcessorContext context, StateStore root) {
            this.context = context;
            
            this.serdes = new StateSerdes<>
                (ProcessorStateManager.storeChangelogTopic
                 (context.applicationId(), name()),
                 (Serde<String>) context.keySerde(),
                 (Serde<String>) context.valueSerde());
                    
            this.collector = ((RecordCollector.Supplier) context).recordCollector();

            if (root != null) {
                context.register(root, true, new StateRestoreCallback() {
                        @Override
                        public void restore(byte[] key, byte[] value) {
                            put(serdes.keyFrom(key),
                                value == null
                                ? null
                                : serdes.valueFrom(value));
                        }
                    });
            }            
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public boolean persistent() {
            return false;
        }

        @Override
        public String name() {
            return name;
        }

        private static class Supplier implements StateStoreSupplier<MapStore> {
            private final String name;
            private final Map<String, String> map;

            public Supplier(String name, Map<String, String> map) {
                this.name = name;
                this.map = map;
            }
            
            @Override
            public MapStore get() {
                return new MapStore(name, map);
            }

            @Override
            public Map<String, String> logConfig() {
                return Collections.emptyMap();
            }

            @Override
            public boolean loggingEnabled() {
                return true;
            }

            @Override
            public String name() {
                return name;
            }
        }
    }
}
