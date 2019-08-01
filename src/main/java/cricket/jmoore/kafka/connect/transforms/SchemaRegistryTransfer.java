/* Licensed under Apache-2.0 */
package cricket.jmoore.kafka.connect.transforms;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;

@SuppressWarnings("unused")
public class SchemaRegistryTransfer<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC = "Inspect the Confluent KafkaAvroSerializer's wire-format header to copy schemas from one Schema Registry to another.";
    private static final Logger log = LoggerFactory.getLogger(SchemaRegistryTransfer.class);

    private static final byte MAGIC_BYTE = (byte) 0x0;
    // wire-format is magic byte + an integer, then data
    private static final short WIRE_FORMAT_PREFIX_LENGTH = 1 + (Integer.SIZE / Byte.SIZE);

    public static final ConfigDef CONFIG_DEF;
    public static final String SCHEMA_CAPACITY_CONFIG_DOC = "The maximum amount of schemas to be stored for each Schema Registry client.";
    public static final Integer SCHEMA_CAPACITY_CONFIG_DEFAULT = 100;

    public static final String SRC_PREAMBLE = "For source consumer's schema registry, ";
    public static final String SRC_SCHEMA_REGISTRY_CONFIG_DOC = "A list of addresses for the Schema Registry to copy from. The consumer's Schema Registry.";
    public static final String SRC_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DOC = SRC_PREAMBLE + AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE_DOC;
    public static final String SRC_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DEFAULT = AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE_DEFAULT;
    public static final String SRC_USER_INFO_CONFIG_DOC = SRC_PREAMBLE + AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_USER_INFO_DOC;
    public static final String SRC_USER_INFO_CONFIG_DEFAULT = AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_USER_INFO_DEFAULT;

    public static final String DEST_PREAMBLE = "For target producer's schema registry, ";
    public static final String DEST_SCHEMA_REGISTRY_CONFIG_DOC = "A list of addresses for the Schema Registry to copy to. The producer's Schema Registry.";
    public static final String DEST_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DOC = DEST_PREAMBLE + AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE_DOC;
    public static final String DEST_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DEFAULT = AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE_DEFAULT;
    public static final String DEST_USER_INFO_CONFIG_DOC = DEST_PREAMBLE + AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_USER_INFO_DOC;
    public static final String DEST_USER_INFO_CONFIG_DEFAULT = AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_USER_INFO_DEFAULT;

    public static final String TRANSFER_KEYS_CONFIG_DOC = "Whether or not to copy message key schemas between registries.";
    public static final Boolean TRANSFER_KEYS_CONFIG_DEFAULT = true;
    public static final String INCLUDE_HEADERS_CONFIG_DOC = "Whether or not to preserve the Kafka Connect Record headers.";
    public static final Boolean INCLUDE_HEADERS_CONFIG_DEFAULT = true;

    private CachedSchemaRegistryClient sourceSchemaRegistryClient;
    private CachedSchemaRegistryClient destSchemaRegistryClient;
    private SubjectNameStrategy<org.apache.avro.Schema> subjectNameStrategy;
    private boolean transferKeys, includeHeaders;

    // caches from the source registry to the destination registry
    private Cache<Integer, SchemaAndId> schemaCache;

    public SchemaRegistryTransfer() {
    }

    static {
        CONFIG_DEF = (new ConfigDef())
                .define(ConfigName.SRC_SCHEMA_REGISTRY_URL, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyListValidator(), ConfigDef.Importance.HIGH, SRC_SCHEMA_REGISTRY_CONFIG_DOC)
                .define(ConfigName.DEST_SCHEMA_REGISTRY_URL, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyListValidator(), ConfigDef.Importance.HIGH, DEST_SCHEMA_REGISTRY_CONFIG_DOC)
                .define(ConfigName.SRC_BASIC_AUTH_CREDENTIALS_SOURCE, ConfigDef.Type.STRING, SRC_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DEFAULT, ConfigDef.Importance.MEDIUM, SRC_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DOC)
                .define(ConfigName.SRC_USER_INFO, ConfigDef.Type.STRING, SRC_USER_INFO_CONFIG_DEFAULT, ConfigDef.Importance.MEDIUM, SRC_USER_INFO_CONFIG_DOC)
                .define(ConfigName.DEST_BASIC_AUTH_CREDENTIALS_SOURCE, ConfigDef.Type.STRING, DEST_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DEFAULT, ConfigDef.Importance.MEDIUM, DEST_BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG_DOC)
                .define(ConfigName.DEST_USER_INFO, ConfigDef.Type.STRING, DEST_USER_INFO_CONFIG_DEFAULT, ConfigDef.Importance.MEDIUM, DEST_USER_INFO_CONFIG_DOC)
                .define(ConfigName.SCHEMA_CAPACITY, ConfigDef.Type.INT, SCHEMA_CAPACITY_CONFIG_DEFAULT, ConfigDef.Importance.LOW, SCHEMA_CAPACITY_CONFIG_DOC)
                .define(ConfigName.TRANSFER_KEYS, ConfigDef.Type.BOOLEAN, TRANSFER_KEYS_CONFIG_DEFAULT, ConfigDef.Importance.MEDIUM, TRANSFER_KEYS_CONFIG_DOC)
                .define(ConfigName.INCLUDE_HEADERS, ConfigDef.Type.BOOLEAN, INCLUDE_HEADERS_CONFIG_DEFAULT, ConfigDef.Importance.MEDIUM, INCLUDE_HEADERS_CONFIG_DOC)
        ;
        // TODO: Other properties might be useful, e.g. the Subject Strategies
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> props) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

        List<String> sourceUrls = config.getList(ConfigName.SRC_SCHEMA_REGISTRY_URL);
        final Map<String, String> sourceProps = new HashMap<>();
        sourceProps.put(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,
            config.getString(ConfigName.SRC_BASIC_AUTH_CREDENTIALS_SOURCE));
        sourceProps.put(AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG,
            config.getString(ConfigName.SRC_USER_INFO));

        List<String> destUrls = config.getList(ConfigName.DEST_SCHEMA_REGISTRY_URL);
        final Map<String, String> destProps = new HashMap<>();
        destProps.put(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,
            config.getString(ConfigName.DEST_BASIC_AUTH_CREDENTIALS_SOURCE));
        destProps.put(AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG,
            config.getString(ConfigName.DEST_USER_INFO));

        Integer schemaCapacity = config.getInt(ConfigName.SCHEMA_CAPACITY);

        this.schemaCache = new SynchronizedCache<>(new LRUCache<>(schemaCapacity));
        this.sourceSchemaRegistryClient = new CachedSchemaRegistryClient(sourceUrls, schemaCapacity, sourceProps);
        this.destSchemaRegistryClient = new CachedSchemaRegistryClient(destUrls, schemaCapacity, destProps);

        this.transferKeys = config.getBoolean(ConfigName.TRANSFER_KEYS);
        this.includeHeaders = config.getBoolean(ConfigName.INCLUDE_HEADERS);

        // TODO: Make the Strategy configurable, may be different for src and dest
        // Strategy for the -key and -value subjects
        this.subjectNameStrategy = new TopicNameStrategy();
    }

    @Override
    public R apply(R r) {
        final String topic = r.topic();

        // Transcribe the key's schema id
        final Object key = r.key();
        final Schema keySchema = r.keySchema();

        Object updatedKey = key;
        Optional<Integer> destKeySchemaId;
        if (transferKeys) {
            if (ConnectSchemaUtil.isBytesSchema(keySchema) || key instanceof byte[]) {
                if (key == null) {
                    log.trace("Passing through null record key.");
                } else {
                    byte[] keyAsBytes = (byte[]) key;
                    int keyByteLength = keyAsBytes.length;
                    if (keyByteLength <= 5) {
                        throw new SerializationException("Unexpected byte[] length " + keyByteLength + " for Avro record key.");
                    }
                    ByteBuffer b = ByteBuffer.wrap(keyAsBytes);
                    destKeySchemaId = copySchema(b, topic, true);
                    b.putInt(1, destKeySchemaId.orElseThrow(()
                            -> new ConnectException("Transform failed. Unable to update record schema id. (isKey=true)")));
                    updatedKey = b.array();
                }
            } else {
                throw new ConnectException("Transform failed. Record key does not have a byte[] schema.");
            }
        } else {
            log.trace("Skipping record key translation. {} has been to false. Keys will be passed as-is."
                    , ConfigName.TRANSFER_KEYS);
        }

        // Transcribe the value's schema id
        final Object value = r.value();
        final Schema valueSchema = r.valueSchema();

        Object updatedValue = value;
        Optional<Integer> destValueSchemaId;
        if (ConnectSchemaUtil.isBytesSchema(valueSchema) || value instanceof byte[]) {
            if (value == null) {
                log.trace("Passing through null record value");
            } else {
                byte[] valueAsBytes = (byte[]) value;
                int valueByteLength = valueAsBytes.length;
                if (valueByteLength <= 5) {
                    throw new SerializationException("Unexpected byte[] length " + valueByteLength + " for Avro record value.");
                }
                ByteBuffer b = ByteBuffer.wrap(valueAsBytes);
                destValueSchemaId = copySchema(b, topic, false);
                b.putInt(1, destValueSchemaId.orElseThrow(()
                        -> new ConnectException("Transform failed. Unable to update record schema id. (isKey=false)")));
                updatedValue = b.array();
            }
        } else {
            throw new ConnectException("Transform failed. Record value does not have a byte[] schema.");
        }


        return includeHeaders ?
                r.newRecord(topic, r.kafkaPartition(),
                        keySchema, updatedKey,
                        valueSchema, updatedValue,
                        r.timestamp(),
                        r.headers())
                :
                r.newRecord(topic, r.kafkaPartition(),
                        keySchema, updatedKey,
                        valueSchema, updatedValue,
                        r.timestamp());
    }

    protected Optional<Integer> copySchema(ByteBuffer buffer, String topic, boolean isKey) {
        SchemaAndId schemaAndDestId;
        if (buffer.get() == MAGIC_BYTE) {
            int sourceSchemaId = buffer.getInt();

            schemaAndDestId = schemaCache.get(sourceSchemaId);
            if (schemaAndDestId != null) {
                log.trace("Schema id {} has been seen before. Not registering with destination registry again.");
            } else { // cache miss
                log.trace("Schema id {} has not been seen before", sourceSchemaId);
                schemaAndDestId = new SchemaAndId();
                try {
                    log.trace("Looking up schema id {} in source registry", sourceSchemaId);
                    // Can't do getBySubjectAndId because that requires a Schema object for the strategy
                    schemaAndDestId.schema = sourceSchemaRegistryClient.getById(sourceSchemaId);
                } catch (IOException | RestClientException e) {
                    log.error(String.format("Unable to fetch source schema for id %d.", sourceSchemaId), e);
                    throw new ConnectException(e);
                }

                try {
                    log.trace("Registering schema {} to destination registry", schemaAndDestId.schema);
                    // It could be possible that the destination naming strategy is different from the source
                    String subjectName = subjectNameStrategy.subjectName(topic, isKey, schemaAndDestId.schema);
                    schemaAndDestId.id = destSchemaRegistryClient.register(subjectName, schemaAndDestId.schema);
                    schemaCache.put(sourceSchemaId, schemaAndDestId);
                } catch (IOException | RestClientException e) {
                    log.error(String.format("Unable to register source schema id %d to destination registry.",
                            sourceSchemaId), e);
                    return Optional.empty();
                }
            }
        } else {
            throw new SerializationException("Unknown magic byte!");
        }
        return Optional.ofNullable(schemaAndDestId.id);
    }

    @Override
    public void close() {
        this.sourceSchemaRegistryClient = null;
        this.destSchemaRegistryClient = null;
    }

    interface ConfigName {
        String SRC_SCHEMA_REGISTRY_URL = "src." + AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
        String SRC_BASIC_AUTH_CREDENTIALS_SOURCE = "src." + AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
        String SRC_USER_INFO = "src." + AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG;
        String DEST_SCHEMA_REGISTRY_URL = "dest." + AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
        String DEST_BASIC_AUTH_CREDENTIALS_SOURCE = "dest." + AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
        String DEST_USER_INFO = "dest." + AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG;
        String SCHEMA_CAPACITY = "schema.capacity";
        String TRANSFER_KEYS = "transfer.message.keys";
        String INCLUDE_HEADERS = "include.message.headers";
    }

    private static class SchemaAndId {
        private Integer id;
        private org.apache.avro.Schema schema;

        SchemaAndId() {
        }

        SchemaAndId(int id, org.apache.avro.Schema schema) {
            this.id = id;
            this.schema = schema;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SchemaAndId schemaAndId = (SchemaAndId) o;
            return Objects.equals(id, schemaAndId.id) &&
                    Objects.equals(schema, schemaAndId.schema);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, schema);
        }

        @Override
        public String toString() {
            return "SchemaAndId{" +
                    "id=" + id +
                    ", schema=" + schema +
                    '}';
        }
    }

}
