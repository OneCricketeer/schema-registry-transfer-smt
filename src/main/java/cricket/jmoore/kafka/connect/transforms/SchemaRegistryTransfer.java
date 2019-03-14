/* Licensed under Apache-2.0 */
package cricket.jmoore.kafka.connect.transforms;

import java.io.IOException;
import java.nio.ByteBuffer;
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

    private static final byte MAGIC_BYTE = 0x0;

    public static final ConfigDef CONFIG_DEF;
    public static final String SCHEMA_CAPACITY_CONFIG_DOC = "The maximum amount of schemas to be stored for each Schema Registry client.";
    public static final Integer SCHEMA_CAPACITY_CONFIG_DEFAULT = 100;
    public static final String SRC_SCHEMA_REGISTRY_CONFIG_DOC = "A list of addresses for the Schema Registry to copy from. The consumer's Schema Registry.";
    public static final String DEST_SCHEMA_REGISTRY_CONFIG_DOC = "A list of addresses for the Schema Registry to copy to. The producer's Schema Registry.";
    public static final String INCLUDE_KEYS_CONFIG_DOC = "Whether or not to copy message key schemas between registries.";
    public static final Boolean INCLUDE_KEYS_CONFIG_DEFAULT = true;
    public static final String INCLUDE_HEADERS_CONFIG_DOC = "Whether or not to preserve the Kafka Connect Record headers.";
    public static final Boolean INCLUDE_HEADERS_CONFIG_DEFAULT = true;

    private CachedSchemaRegistryClient sourceSchemaRegistryClient;
    private CachedSchemaRegistryClient destSchemaRegistryClient;
    private SubjectNameStrategy<org.apache.avro.Schema> subjectNameStrategy;
    private boolean includeKeys, includeHeaders;

    // caches from the source registry to the destination registry
    private Cache<Integer, SchemaAndId> schemaCache;

    public SchemaRegistryTransfer() {
    }

    static {
        CONFIG_DEF = (new ConfigDef())
                .define(ConfigName.SRC_SCHEMA_REGISTRY_URL, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyListValidator(), ConfigDef.Importance.HIGH, SRC_SCHEMA_REGISTRY_CONFIG_DOC)
                .define(ConfigName.DEST_SCHEMA_REGISTRY_URL, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyListValidator(), ConfigDef.Importance.HIGH, DEST_SCHEMA_REGISTRY_CONFIG_DOC)
                .define(ConfigName.SCHEMA_CAPACITY, ConfigDef.Type.INT, SCHEMA_CAPACITY_CONFIG_DEFAULT, ConfigDef.Importance.LOW, SCHEMA_CAPACITY_CONFIG_DOC)
                .define(ConfigName.INCLUDE_KEYS, ConfigDef.Type.BOOLEAN, INCLUDE_KEYS_CONFIG_DEFAULT, ConfigDef.Importance.MEDIUM, INCLUDE_KEYS_CONFIG_DOC)
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
        List<String> destUrls = config.getList(ConfigName.DEST_SCHEMA_REGISTRY_URL);
        Integer schemaCapacity = config.getInt(ConfigName.SCHEMA_CAPACITY);

        this.schemaCache = new SynchronizedCache<>(new LRUCache<>(schemaCapacity));
        this.sourceSchemaRegistryClient = new CachedSchemaRegistryClient(sourceUrls, schemaCapacity);
        this.destSchemaRegistryClient = new CachedSchemaRegistryClient(destUrls, schemaCapacity);

        this.includeKeys = config.getBoolean(ConfigName.INCLUDE_KEYS);
        this.includeHeaders = config.getBoolean(ConfigName.INCLUDE_HEADERS);

        // TODO: Make the Strategy configurable, may be different for src and dest
        // Strategy for the -key and -value subjects
        this.subjectNameStrategy = new TopicNameStrategy();
    }

    @Override
    public R apply(R r) {
        final String topic = r.topic();

        // Transcribe the key's schema id
        Object key = r.key();
        Schema keySchema = r.keySchema();

        Object updatedKey = key;
        Optional<Integer> destKeySchemaId;
        if (includeKeys) {
            if (key == null) {
                throw new ConnectException("Unable to copy record key schemas for null keys. Set '" + ConfigName.INCLUDE_KEYS + "=false'.");
            }
            if ((keySchema != null && keySchema.type() == Schema.BYTES_SCHEMA.type()) ||
                    key instanceof byte[]) {
                ByteBuffer b = ByteBuffer.wrap((byte[]) key);
                destKeySchemaId = copySchema(b, topic, true);
                b.putInt(1, destKeySchemaId.orElseThrow(()
                        -> new ConnectException("Transform failed. Unable to update record schema id. (isKey=true)")));
                updatedKey = b.array();
            } else {
                throw new ConnectException("Transform failed. Record key does not have a byte[] schema.");
            }
        }

        // Transcribe the value's schema id
        Object value = r.value();
        Schema valueSchema = r.valueSchema();

        Object updatedValue;
        Optional<Integer> destValueSchemaId;
        if (value == null) {
            throw new ConnectException("Unable to extract schema information from null record value.");
        }
        if ((valueSchema != null && valueSchema.type() == Schema.BYTES_SCHEMA.type()) ||
                value instanceof byte[]) {
            ByteBuffer b = ByteBuffer.wrap((byte[]) value);
            destValueSchemaId = copySchema(b, topic, false);
            b.putInt(1, destValueSchemaId.orElseThrow(()
                    -> new ConnectException("Transform failed. Unable to update record schema id. (isKey=false)")));
            updatedValue = b.array();
        } else {
            throw new ConnectException("Transform failed. Record value does not have a byte[] schema.");
        }


        return includeHeaders ?
                r.newRecord(topic, r.kafkaPartition(),
                        keySchema, includeKeys ? updatedKey : r.key(),
                        valueSchema, updatedValue,
                        r.timestamp(),
                        r.headers())
                :
                r.newRecord(topic, r.kafkaPartition(),
                        keySchema, includeKeys ? updatedKey : r.key(),
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

                if (schemaAndDestId.schema == null) {
                    String msg = "Error getting schema from source registry. Not registering null schema with destination registry.";
                    log.error(msg);
                    throw new ConnectException(msg);
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
        String DEST_SCHEMA_REGISTRY_URL = "dest." + AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
        String SCHEMA_CAPACITY = "schema.capacity";
        String INCLUDE_KEYS = "include.message.keys";
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
