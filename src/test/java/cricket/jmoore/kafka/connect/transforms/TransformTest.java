/* Licensed under Apache-2.0 */
package cricket.jmoore.kafka.connect.transforms;

import static cricket.jmoore.kafka.connect.transforms.SchemaRegistryTransfer.ConfigName;
import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.NonRecordContainer;

import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.BOOLEAN;
import static org.apache.avro.Schema.Type.STRING;

@SuppressWarnings("unchecked")
public class TransformTest {

    private enum ExplicitAuthType {
        USER_INFO,
        URL,
        NULL;
    }

    private static final Logger log = LoggerFactory.getLogger(TransformTest.class);

    public static final String TOPIC = TransformTest.class.getSimpleName();

    private static final byte MAGIC_BYTE = (byte) 0x0;
    public static final int ID_SIZE = Integer.SIZE / Byte.SIZE;
    private static final int AVRO_CONTENT_OFFSET = 1 + ID_SIZE;
    public static final org.apache.avro.Schema INT_SCHEMA = org.apache.avro.Schema.create(INT);
    public static final org.apache.avro.Schema STRING_SCHEMA = org.apache.avro.Schema.create(STRING);
    public static final org.apache.avro.Schema BOOLEAN_SCHEMA = org.apache.avro.Schema.create(BOOLEAN);
    public static final org.apache.avro.Schema NAME_SCHEMA = SchemaBuilder.record("FullName")
            .namespace("cricket.jmoore.kafka.connect.transforms").fields()
            .requiredString("first")
            .requiredString("last")
            .endRecord();
    public static final org.apache.avro.Schema NAME_SCHEMA_ALIASED = SchemaBuilder.record("FullName")
            .namespace("cricket.jmoore.kafka.connect.transforms").fields()
            .requiredString("first")
            .name("surname").aliases("last").type().stringType().noDefault()
            .endRecord();

    @RegisterExtension
    final SchemaRegistryMock sourceSchemaRegistry =
        new SchemaRegistryMock(SchemaRegistryMock.Role.SOURCE);

    @RegisterExtension
    final SchemaRegistryMock destSchemaRegistry =
        new SchemaRegistryMock(SchemaRegistryMock.Role.DESTINATION);

    private SchemaRegistryTransfer smt;
    private Map<String, Object> smtConfiguration;

    private ConnectRecord createRecord(Schema keySchema, Object key, Schema valueSchema, Object value) {
        // partition and offset aren't needed
        return new SourceRecord(null, null, TOPIC, keySchema, key, valueSchema, value);
    }

    private ConnectRecord createRecord(byte[] key, byte[] value) {
        return createRecord(Schema.OPTIONAL_BYTES_SCHEMA, key, Schema.OPTIONAL_BYTES_SCHEMA, value);
    }

    private Map<String, Object> getRequiredTransformConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConfigName.SRC_SCHEMA_REGISTRY_URL, sourceSchemaRegistry.getUrl());
        configs.put(ConfigName.DEST_SCHEMA_REGISTRY_URL, destSchemaRegistry.getUrl());
        return configs;
    }

    private void configure(boolean copyKeys) {
        smtConfiguration.put(ConfigName.TRANSFER_KEYS, copyKeys);
        smt.configure(smtConfiguration);
    }

    private void configure(boolean copyKeys, boolean copyHeaders) {
        smtConfiguration.put(ConfigName.TRANSFER_KEYS, copyKeys);
        smtConfiguration.put(ConfigName.INCLUDE_HEADERS, copyHeaders);
        smt.configure(smtConfiguration);
    }

    private void configure(final String sourceUserInfo, final String destUserInfo, ExplicitAuthType credentialSource) {
        if (credentialSource == ExplicitAuthType.USER_INFO) {
            if (sourceUserInfo != null) {
                smtConfiguration.put(ConfigName.SRC_BASIC_AUTH_CREDENTIALS_SOURCE, Constants.USER_INFO_SOURCE);
                smtConfiguration.put(ConfigName.SRC_USER_INFO, sourceUserInfo);
            }

            if (destUserInfo != null) {
                smtConfiguration.put(ConfigName.DEST_BASIC_AUTH_CREDENTIALS_SOURCE, Constants.USER_INFO_SOURCE);
                smtConfiguration.put(ConfigName.DEST_USER_INFO, destUserInfo);
            }
        } else {
            if (sourceUserInfo != null) {
                String url = sourceSchemaRegistry.getUrl();
                url = url.replace("://", "://" + sourceUserInfo + "@" );
                smtConfiguration.put(ConfigName.SRC_SCHEMA_REGISTRY_URL, url);

                if (credentialSource == ExplicitAuthType.URL) {
                    smtConfiguration.put(ConfigName.SRC_BASIC_AUTH_CREDENTIALS_SOURCE, Constants.URL_SOURCE);
                } else if (credentialSource == ExplicitAuthType.NULL) {
                    // For an explicit null case, set both the URL and UserInfo to confirm that neither is found
                    smtConfiguration.put(ConfigName.SRC_BASIC_AUTH_CREDENTIALS_SOURCE, null);
                    smtConfiguration.put(ConfigName.SRC_USER_INFO, sourceUserInfo);
                } else {
                    // For null ExplicitAuthType, insert no key and rely on implicit default.
                }
            }

            if (destUserInfo != null) {
                String url = destSchemaRegistry.getUrl();
                url = url.replace("://", "://" + destUserInfo + "@" );
                smtConfiguration.put(ConfigName.DEST_SCHEMA_REGISTRY_URL, url);

                if (credentialSource == ExplicitAuthType.URL) {
                    smtConfiguration.put(ConfigName.DEST_BASIC_AUTH_CREDENTIALS_SOURCE, Constants.URL_SOURCE);
                } else if (credentialSource == ExplicitAuthType.NULL) {
                    // For an explicit null case, set both the URL and UserInfo to confirm that neither is found
                    smtConfiguration.put(ConfigName.DEST_BASIC_AUTH_CREDENTIALS_SOURCE, null);
                    smtConfiguration.put(ConfigName.DEST_USER_INFO, destUserInfo);
                } else {
                    // For null ExplicitAuthType, insert no key and rely on implicit default.
                }
            }
        }

        smt.configure(smtConfiguration);
    }

    private ByteArrayOutputStream encodeAvroObject(org.apache.avro.Schema schema, int sourceId, Object datum) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        out.write(MAGIC_BYTE);
        out.write(ByteBuffer.allocate(ID_SIZE).putInt(sourceId).array());

        EncoderFactory encoderFactory = EncoderFactory.get();
        BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
        Object
                value =
                datum instanceof NonRecordContainer ? ((NonRecordContainer) datum).getValue()
                        : datum;
        DatumWriter<Object> writer = new GenericDatumWriter<>(schema);
        writer.write(value, encoder);
        encoder.flush();

        return out;
    }

    // Used to run a message through the SMT when testing authentication modes, which only need to
    // know if there was a communication error, but rely on other tests to verify schema transfers
    // are making the correct API calls.
    private void passSimpleMessage() throws IOException {
        // Create key/value schemas for source registry
        log.info("Registering key/value string schemas in source registry");
        final int sourceKeyId = sourceSchemaRegistry.registerSchema(TOPIC, true, STRING_SCHEMA);
        final int sourceValId = sourceSchemaRegistry.registerSchema(TOPIC, false, STRING_SCHEMA);

        final ByteArrayOutputStream keyOut =
            encodeAvroObject(STRING_SCHEMA, sourceKeyId, Constants.HELLO_WORLD_VALUE);
        final ByteArrayOutputStream valOut =
            encodeAvroObject(STRING_SCHEMA, sourceValId, Constants.HELLO_WORLD_VALUE);
        final ConnectRecord record =
            createRecord(keyOut.toByteArray(), valOut.toByteArray());

        smt.apply(record);
    }

    @BeforeEach
    public void setup() {
        smt = new SchemaRegistryTransfer();
        smtConfiguration = getRequiredTransformConfigs();
    }

    @Test
    public void applyKeySchemaNotBytes() {
        configure(true);

        ConnectRecord record = createRecord(null, null, null, null);

        // The key schema is not a byte[]
        assertThrows(ConnectException.class, () -> smt.apply(record));
    }

    @Test
    public void applyValueSchemaNotBytes() {
        configure(false);

        ConnectRecord record = createRecord(null, null, null, null);

        // The value schema is not a byte[]
        assertThrows(ConnectException.class, () -> smt.apply(record));
    }

    @Test
    public void applySchemalessKeyBytesTooShort() {
        configure(true);

        // allocate enough space for the magic-byte
        byte[] b = ByteBuffer.allocate(1).array();
        ConnectRecord record = createRecord(null, b, null, null);

        // The key payload is not long enough for schema registry wire-format
        assertThrows(SerializationException.class, () -> smt.apply(record));
    }

    @Test
    public void applySchemalessValueBytesTooShort() {
        configure(false);

        // allocate enough space for the magic-byte
        byte[] b = ByteBuffer.allocate(1).array();
        ConnectRecord record = createRecord(null, null, null, b);

        // The value payload is not long enough for schema registry wire-format
        assertThrows(SerializationException.class, () -> smt.apply(record));
    }

    @Test
    public void testKeySchemaLookupFailure() {
        configure(true);

        byte[] b = ByteBuffer.allocate(6).array();
        ConnectRecord record = createRecord(null, b, null, null);

        // tries to lookup schema id 0, but that isn't a valid id
        assertThrows(ConnectException.class, () -> smt.apply(record));
    }

    @Test
    public void testValueSchemaLookupFailure() {
        configure(false);

        byte[] b = ByteBuffer.allocate(6).array();
        ConnectRecord record = createRecord(null, null, null, b);

        // tries to lookup schema id 0, but that isn't a valid id
        assertThrows(ConnectException.class, () -> smt.apply(record));
    }

    @Test
    @Tag(Constants.USE_BASIC_AUTH_SOURCE_TAG)
    public void testSourceBasicHttpAuthUserInfo() throws IOException {
        configure(Constants.HTTP_AUTH_CREDENTIALS_FIXTURE, null, ExplicitAuthType.USER_INFO);

        this.passSimpleMessage();
    }

    @Test
    @Tag(Constants.USE_BASIC_AUTH_DEST_TAG)
    public void testDestinationBasicHttpAuthUserInfo() throws IOException {
        configure(null, Constants.HTTP_AUTH_CREDENTIALS_FIXTURE, ExplicitAuthType.USER_INFO);

        this.passSimpleMessage();
    }

    @Test
    @Tag(Constants.USE_BASIC_AUTH_SOURCE_TAG)
    public void testSourceBasicHttpAuthUrl() throws IOException {
        configure(Constants.HTTP_AUTH_CREDENTIALS_FIXTURE, null, ExplicitAuthType.URL);

        this.passSimpleMessage();
    }

    @Test
    @Tag(Constants.USE_BASIC_AUTH_DEST_TAG)
    public void testDestinationBasicHttpAuthUrl() throws IOException {
        configure(null, Constants.HTTP_AUTH_CREDENTIALS_FIXTURE, ExplicitAuthType.URL);

        this.passSimpleMessage();
    }

    @Test
    @Tag(Constants.USE_BASIC_AUTH_SOURCE_TAG)
    public void testSourceBasicHttpAuthNull() throws IOException {
        configure(Constants.HTTP_AUTH_CREDENTIALS_FIXTURE, null, ExplicitAuthType.NULL);

       assertThrows(ConnectException.class, () -> this.passSimpleMessage());
    }

    @Test
    @Tag(Constants.USE_BASIC_AUTH_DEST_TAG)
    public void testDestinationBasicHttpAuthNull() throws IOException {
        configure(null, Constants.HTTP_AUTH_CREDENTIALS_FIXTURE, ExplicitAuthType.NULL);

        assertThrows(ConnectException.class, () -> this.passSimpleMessage());
    }

    @Test
    @Tag(Constants.USE_BASIC_AUTH_SOURCE_TAG)
    public void testSourceBasicHttpAuthImplicitDefault() throws IOException {
        configure(Constants.HTTP_AUTH_CREDENTIALS_FIXTURE, null, null);

        this.passSimpleMessage();
    }

    @Test
    @Tag(Constants.USE_BASIC_AUTH_DEST_TAG)
    public void testDestinationBasicHttpAuthImplicitDefault() throws IOException {
        configure(null, Constants.HTTP_AUTH_CREDENTIALS_FIXTURE, null);

        this.passSimpleMessage();
    }

    @Test
    @Tag(Constants.USE_BASIC_AUTH_SOURCE_TAG)
    public void testSourceBasicHttpAuthOmit() throws IOException {
        configure(null, null, null);

        assertThrows(ConnectException.class, () -> this.passSimpleMessage());
    }

    @Test
    @Tag(Constants.USE_BASIC_AUTH_DEST_TAG)
    public void testDestinationBasicHttpAuthOmit() throws IOException {
        configure(null, null, null);

        assertThrows(ConnectException.class, () -> this.passSimpleMessage());
    }

    @Test
    public void testKeySchemaTransfer() {
        configure(true);

        // Create bogus schema in destination so that source and destination ids differ
        log.info("Registering schema in destination registry");
        destSchemaRegistry.registerSchema(UUID.randomUUID().toString(), true, INT_SCHEMA);

        // Create new schema for source registry
        org.apache.avro.Schema schema = STRING_SCHEMA;
        log.info("Registering schema in source registry");
        int sourceId = sourceSchemaRegistry.registerSchema(TOPIC, true, schema);
        final String subject = TOPIC + "-key";
        assertEquals(1, sourceId, "An empty registry starts at id=1");

        SchemaRegistryClient sourceClient = sourceSchemaRegistry.getSchemaRegistryClient();
        int numSourceVersions = 0;
        try {
            numSourceVersions = sourceClient.getAllVersions(subject).size();
            assertEquals(1, numSourceVersions, "the source registry subject contains the pre-registered schema");
        } catch (IOException | RestClientException e) {
            fail(e);
        }

        try {
            ByteArrayOutputStream out = encodeAvroObject(schema, sourceId, "hello, world");

            ConnectRecord record = createRecord(Schema.OPTIONAL_BYTES_SCHEMA, out.toByteArray(), null, null);

            // check the destination has no versions for this subject
            SchemaRegistryClient destClient = destSchemaRegistry.getSchemaRegistryClient();
            List<Integer> destVersions = destClient.getAllVersions(subject);
            assertTrue(destVersions.isEmpty(), "the destination registry starts empty");

            // The transform will fail on the byte[]-less record value.
            // TODO: Allow only key schemas to be copied?
            log.info("applying transformation");
            ConnectException connectException = assertThrows(ConnectException.class, () -> smt.apply(record));
            assertEquals("Transform failed. Record value does not have a byte[] schema.", connectException.getMessage());

            // In any case, we can still check the key schema was copied, and the destination now has some version
            destVersions = destClient.getAllVersions(subject);
            assertEquals(numSourceVersions, destVersions.size(),
                    "source and destination registries have the same amount of schemas for the same subject");

            // Verify that the ids for the source and destination are different
            SchemaMetadata metadata = destClient.getSchemaMetadata(subject, destVersions.get(0));
            int destinationId = metadata.getId();
            log.debug("source_id={} ; dest_id={}", sourceId, destinationId);
            assertTrue(sourceId < destinationId,
                    "destination id should be different and higher since that registry already had schemas");

            // Verify the schema is the same
            org.apache.avro.Schema sourceSchema = sourceClient.getById(sourceId);
            org.apache.avro.Schema destSchema = new org.apache.avro.Schema.Parser().parse(metadata.getSchema());
            assertEquals(schema, sourceSchema, "source server returned same schema");
            assertEquals(schema, destSchema, "destination server returned same schema");
            assertEquals(sourceSchema, destSchema, "both servers' schemas match");
        } catch (IOException | RestClientException e) {
            fail(e);
        }
    }

    @Test
    public void testValueSchemaTransfer() {
        configure(true);

        // Create bogus schema in destination so that source and destination ids differ
        log.info("Registering schema in destination registry");
        destSchemaRegistry.registerSchema(UUID.randomUUID().toString(), false, INT_SCHEMA);

        // Create new schema for source registry
        org.apache.avro.Schema schema = STRING_SCHEMA;
        log.info("Registering schema in source registry");
        int sourceId = sourceSchemaRegistry.registerSchema(TOPIC, false, schema);
        final String subject = TOPIC + "-value";
        assertEquals(1, sourceId, "An empty registry starts at id=1");

        SchemaRegistryClient sourceClient = sourceSchemaRegistry.getSchemaRegistryClient();
        int numSourceVersions = 0;
        try {
            numSourceVersions = sourceClient.getAllVersions(subject).size();
            assertEquals(1, numSourceVersions, "the source registry subject contains the pre-registered schema");
        } catch (IOException | RestClientException e) {
            fail(e);
        }

        byte[] value = null;
        ConnectRecord appliedRecord = null;
        int destinationId = -1;
        try {
            ByteArrayOutputStream out = encodeAvroObject(schema, sourceId, "hello, world");

            value = out.toByteArray();
            ConnectRecord record = createRecord(null, value);

            // check the destination has no versions for this subject
            SchemaRegistryClient destClient = destSchemaRegistry.getSchemaRegistryClient();
            List<Integer> destVersions = destClient.getAllVersions(subject);
            assertTrue(destVersions.isEmpty(), "the destination registry starts empty");

            // The transform will pass for key and value with byte schemas
            log.info("applying transformation");
            appliedRecord = assertDoesNotThrow(() -> smt.apply(record));

            assertEquals(record.keySchema(), appliedRecord.keySchema(), "key schema unchanged");
            assertEquals(record.key(), appliedRecord.key(), "null key not modified");
            assertEquals(record.valueSchema(), appliedRecord.valueSchema(), "value schema unchanged");

            // check the value schema was copied, and the destination now has some version
            destVersions = destClient.getAllVersions(subject);
            assertEquals(numSourceVersions, destVersions.size(),
                    "source and destination registries have the same amount of schemas for the same subject");

            // Verify that the ids for the source and destination are different
            SchemaMetadata metadata = destClient.getSchemaMetadata(subject, destVersions.get(0));
            destinationId = metadata.getId();
            log.debug("source_id={} ; dest_id={}", sourceId, destinationId);
            assertTrue(sourceId < destinationId,
                    "destination id should be different and higher since that registry already had schemas");

            // Verify the schema is the same
            org.apache.avro.Schema sourceSchema = sourceClient.getById(sourceId);
            org.apache.avro.Schema destSchema = new org.apache.avro.Schema.Parser().parse(metadata.getSchema());
            assertEquals(schema, sourceSchema, "source server returned same schema");
            assertEquals(schema, destSchema, "destination server returned same schema");
            assertEquals(sourceSchema, destSchema, "both servers' schemas match");
        } catch (IOException | RestClientException e) {
            fail(e);
        }

        // Verify the record's byte value was transformed, and avro content is same
        byte[] appliedValue = (byte[]) appliedRecord.value();
        ByteBuffer appliedValueBuffer = ByteBuffer.wrap(appliedValue);
        assertEquals(value.length, appliedValue.length, "byte[] values sizes unchanged");
        assertEquals(MAGIC_BYTE, appliedValueBuffer.get(), "record value starts with magic byte");
        int transformedRecordSchemaId = appliedValueBuffer.getInt();
        assertNotEquals(sourceId, transformedRecordSchemaId, "transformed record's schema id changed");
        assertEquals(destinationId, transformedRecordSchemaId, "record value's schema id matches destination id");
        assertArrayEquals(Arrays.copyOfRange(value, AVRO_CONTENT_OFFSET, value.length),
                Arrays.copyOfRange(appliedValueBuffer.array(), AVRO_CONTENT_OFFSET, appliedValue.length),
                "the avro data is not modified");
    }

    @Test
    public void testKeyValueSchemaTransfer() {
        configure(true);

        // Create bogus schema in destination so that source and destination ids differ
        log.info("Registering schema in destination registry");
        destSchemaRegistry.registerSchema(UUID.randomUUID().toString(), false, BOOLEAN_SCHEMA);

        // Create new schemas for source registry
        org.apache.avro.Schema keySchema = INT_SCHEMA;
        org.apache.avro.Schema valueSchema = STRING_SCHEMA;
        log.info("Registering schemas in source registry");
        int sourceKeyId = sourceSchemaRegistry.registerSchema(TOPIC, true, keySchema);
        final String keySubject = TOPIC + "-key";
        assertEquals(1, sourceKeyId, "An empty registry starts at id=1");
        int sourceValueId = sourceSchemaRegistry.registerSchema(TOPIC, false, valueSchema);
        final String valueSubject = TOPIC + "-value";
        assertEquals(2, sourceValueId, "unique schema ids monotonically increase");

        SchemaRegistryClient sourceClient = sourceSchemaRegistry.getSchemaRegistryClient();
        int numSourceKeyVersions = 0;
        int numSourceValueVersions = 0;
        try {
            numSourceKeyVersions = sourceClient.getAllVersions(keySubject).size();
            assertEquals(1, numSourceKeyVersions, "the source registry subject contains the pre-registered key schema");
            numSourceValueVersions = sourceClient.getAllVersions(valueSubject).size();
            assertEquals(1, numSourceValueVersions, "the source registry subject contains the pre-registered value schema");
        } catch (IOException | RestClientException e) {
            fail(e);
        }

        byte[] key = null;
        byte[] value = null;
        ConnectRecord appliedRecord = null;
        int destinationKeyId = -1;
        int destinationValueId = -1;
        try {
            ByteArrayOutputStream keyStream = encodeAvroObject(keySchema, sourceKeyId, AVRO_CONTENT_OFFSET);
            ByteArrayOutputStream valueStream = encodeAvroObject(valueSchema, sourceValueId, "hello, world");

            key = keyStream.toByteArray();
            value = valueStream.toByteArray();
            ConnectRecord record = createRecord(key, value);

            // check the destination has no versions for this subject
            SchemaRegistryClient destClient = destSchemaRegistry.getSchemaRegistryClient();
            List<Integer> destKeyVersions = destClient.getAllVersions(keySubject);
            assertTrue(destKeyVersions.isEmpty(), "the destination registry starts empty");
            List<Integer> destValueVersions = destClient.getAllVersions(valueSubject);
            assertTrue(destValueVersions.isEmpty(), "the destination registry starts empty");

            // The transform will pass for key and value with byte schemas
            log.info("applying transformation");
            appliedRecord = assertDoesNotThrow(() -> smt.apply(record));

            assertEquals(record.keySchema(), appliedRecord.keySchema(), "key schema unchanged");
            assertEquals(record.valueSchema(), appliedRecord.valueSchema(), "value schema unchanged");

            // check the value schema was copied, and the destination now has some version
            destKeyVersions = destClient.getAllVersions(keySubject);
            assertEquals(numSourceKeyVersions, destKeyVersions.size(),
                    "source and destination registries have the same amount of schemas for the key subject");
            destValueVersions = destClient.getAllVersions(valueSubject);
            assertEquals(numSourceValueVersions, destValueVersions.size(),
                    "source and destination registries have the same amount of schemas for the value subject");

            // Verify that the ids for the source and destination are different
            SchemaMetadata keyMetadata = destClient.getSchemaMetadata(keySubject, destKeyVersions.get(0));
            destinationKeyId = keyMetadata.getId();
            log.debug("source_keyId={} ; dest_keyId={}", sourceKeyId, destinationKeyId);
            assertTrue(sourceKeyId < destinationKeyId,
                    "destination id should be different and higher since that registry already had schemas");
            SchemaMetadata valueMetadata = destClient.getSchemaMetadata(valueSubject, destValueVersions.get(0));
            destinationValueId = valueMetadata.getId();
            log.debug("source_valueId={} ; dest_valueId={}", sourceValueId, destinationValueId);
            assertTrue(sourceValueId < destinationValueId,
                    "destination id should be different and higher since that registry already had schemas");

            // Verify the schemas are the same
            org.apache.avro.Schema sourceKeySchema = sourceClient.getById(sourceKeyId);
            org.apache.avro.Schema destKeySchema = new org.apache.avro.Schema.Parser().parse(keyMetadata.getSchema());
            assertEquals(destKeySchema, sourceKeySchema, "source server returned same key schema");
            assertEquals(keySchema, destKeySchema, "destination server returned same key schema");
            assertEquals(sourceKeySchema, destKeySchema, "both servers' key schemas match");
            org.apache.avro.Schema sourceValueSchema = sourceClient.getById(sourceValueId);
            org.apache.avro.Schema destValueSchema = new org.apache.avro.Schema.Parser().parse(valueMetadata.getSchema());
            assertEquals(destValueSchema, sourceValueSchema, "source server returned same value schema");
            assertEquals(valueSchema, destValueSchema, "destination server returned same value schema");
            assertEquals(sourceValueSchema, destValueSchema, "both servers' value schemas match");

        } catch (IOException | RestClientException e) {
            fail(e);
        }

        // Verify the record's byte key was transformed, and avro content is same
        byte[] appliedKey = (byte[]) appliedRecord.key();
        ByteBuffer appliedKeyBuffer = ByteBuffer.wrap(appliedKey);
        assertEquals(key.length, appliedKey.length, "key byte[] sizes unchanged");
        assertEquals(MAGIC_BYTE, appliedKeyBuffer.get(), "record key starts with magic byte");
        int transformedRecordKeySchemaId = appliedKeyBuffer.getInt();
        assertNotEquals(sourceKeyId, transformedRecordKeySchemaId, "transformed record's key schema id changed");
        assertEquals(destinationKeyId, transformedRecordKeySchemaId, "record key's schema id matches destination id");
        assertArrayEquals(Arrays.copyOfRange(key, AVRO_CONTENT_OFFSET, key.length),
                Arrays.copyOfRange(appliedKeyBuffer.array(), AVRO_CONTENT_OFFSET, appliedKey.length),
                "the key's avro data is not modified");

        // Verify the record's byte value was transformed, and avro content is same
        byte[] appliedValue = (byte[]) appliedRecord.value();
        ByteBuffer appliedValueBuffer = ByteBuffer.wrap(appliedValue);
        assertEquals(value.length, appliedValue.length, "value byte[] sizes unchanged");
        assertEquals(MAGIC_BYTE, appliedValueBuffer.get(), "record value starts with magic byte");
        int transformedRecordValueSchemaId = appliedValueBuffer.getInt();
        assertNotEquals(sourceValueId, transformedRecordValueSchemaId, "transformed record's schema id changed");
        assertEquals(destinationValueId, transformedRecordValueSchemaId, "record value's schema id matches destination id");
        assertArrayEquals(Arrays.copyOfRange(value, AVRO_CONTENT_OFFSET, value.length),
                Arrays.copyOfRange(appliedValueBuffer.array(), AVRO_CONTENT_OFFSET, appliedValue.length),
                "the value's avro data is not modified");
    }

    @Test
    public void testTombstoneRecord() {
        configure(false);

        ConnectRecord record = createRecord(null, null, Schema.OPTIONAL_BYTES_SCHEMA, null);

        log.info("applying transformation");
        ConnectRecord appliedRecord = assertDoesNotThrow(() -> smt.apply(record));

        assertEquals(record.valueSchema(), appliedRecord.valueSchema(), "value schema unchanged");
        assertNull(appliedRecord.value());
    }

    @Test
    public void testEvolvingValueSchemaTransfer() {
        configure(true);

        // Create bogus schema in destination so that source and destination ids differ
        log.info("Registering schema in destination registry");
        destSchemaRegistry.registerSchema(UUID.randomUUID().toString(), false, INT_SCHEMA);

        log.info("Registering schema in source registry");
        int sourceId = sourceSchemaRegistry.registerSchema(TOPIC, false, NAME_SCHEMA);
        int nextSourceId = sourceSchemaRegistry.registerSchema(TOPIC, false, NAME_SCHEMA_ALIASED);
        final String subject = TOPIC + "-value";
        assertEquals(1, sourceId, "An empty registry starts at id=1");
        assertEquals(2, nextSourceId, "The next schema is id=2");

        SchemaRegistryClient sourceClient = sourceSchemaRegistry.getSchemaRegistryClient();
        int numSourceVersions = 0;
        try {
            numSourceVersions = sourceClient.getAllVersions(subject).size();
            assertEquals(2, numSourceVersions, "the source registry subject contains the pre-registered schema");
        } catch (IOException | RestClientException e) {
            fail(e);
        }

        try {
            GenericData.Record record1 = new GenericRecordBuilder(NAME_SCHEMA)
                    .set("first", "fname")
                    .set("last", "lname")
                    .build();
            ByteArrayOutputStream out = encodeAvroObject(NAME_SCHEMA, sourceId, record1);

            byte[] value = out.toByteArray();
            ConnectRecord record = createRecord(null, value);

            GenericData.Record record2 = new GenericRecordBuilder(NAME_SCHEMA_ALIASED)
                    .set("first", "fname")
                    .set("surname", "lname")
                    .build();
            out = encodeAvroObject(NAME_SCHEMA_ALIASED, nextSourceId, record2);

            byte[] nextValue = out.toByteArray();
            ConnectRecord nextRecord = createRecord(null, nextValue);

            // check the destination has no versions for this subject
            SchemaRegistryClient destClient = destSchemaRegistry.getSchemaRegistryClient();
            List<Integer> destVersions = destClient.getAllVersions(subject);
            assertTrue(destVersions.isEmpty(), "the destination registry starts empty");

            // The transform will pass for key and value with byte schemas
            log.info("applying transformation");
            assertDoesNotThrow(() -> smt.apply(record));

            // check the value schema was copied, and the destination now has some version
            destVersions = destClient.getAllVersions(subject);
            assertEquals(1, destVersions.size(),
                    "the destination registry has been updated with first schema");

            log.info("applying transformation");
            assertDoesNotThrow(() -> smt.apply(nextRecord));

            destVersions = destClient.getAllVersions(subject);
            assertEquals(numSourceVersions, destVersions.size(),
                    "the destination registry has been updated with the second schema");

        } catch (IOException | RestClientException e) {
            fail(e);
        }
    }

    @Test
    @Disabled("TODO: Find scenario where a backwards compatible change cannot be undone")
    public void testIncompatibleEvolvingValueSchemaTransfer() {
        configure(true);

        // Create bogus schema in destination so that source and destination ids differ
        log.info("Registering schema in destination registry");
        destSchemaRegistry.registerSchema(UUID.randomUUID().toString(), false, INT_SCHEMA);

        // Create new schema for source registry
        log.info("Registering schema in source registry");

        // TODO: Figure out what these should be, where if order is flipped, destination will not accept
        org.apache.avro.Schema schema = null;
        org.apache.avro.Schema nextSchema = null;

        int sourceId = sourceSchemaRegistry.registerSchema(TOPIC, false, schema);
        int nextSourceId = sourceSchemaRegistry.registerSchema(TOPIC, false, nextSchema);
        final String subject = TOPIC + "-value";
        assertEquals(1, sourceId, "An empty registry starts at id=1");
        assertEquals(2, nextSourceId, "The next schema is id=2");

        SchemaRegistryClient sourceClient = sourceSchemaRegistry.getSchemaRegistryClient();
        int numSourceVersions = 0;
        try {
            numSourceVersions = sourceClient.getAllVersions(subject).size();
            assertEquals(2, numSourceVersions, "the source registry subject contains the pre-registered schema");
        } catch (IOException | RestClientException e) {
            fail(e);
        }

        try {
            // TODO: Depending on schemas above, then build Avro records for them
            // ensure second id is encoded first
            ByteArrayOutputStream out = encodeAvroObject(nextSchema, nextSourceId, null);

            byte[] value = out.toByteArray();
            ConnectRecord record = createRecord(null, value);

            out = encodeAvroObject(schema, sourceId, null);

            byte[] nextValue = out.toByteArray();
            ConnectRecord nextRecord = createRecord(null, nextValue);

            // check the destination has no versions for this subject
            SchemaRegistryClient destClient = destSchemaRegistry.getSchemaRegistryClient();
            List<Integer> destVersions = destClient.getAllVersions(subject);
            assertTrue(destVersions.isEmpty(), "the destination registry starts empty");

            // The transform will pass for key and value with byte schemas
            log.info("applying transformation");
            assertDoesNotThrow(() -> smt.apply(record));

            // check the value schema was copied, and the destination now has some version
            destVersions = destClient.getAllVersions(subject);
            assertEquals(1, destVersions.size(),
                    "the destination registry has been updated with first schema");

            log.info("applying transformation");
            assertThrows(ConnectException.class, () -> smt.apply(nextRecord));

        } catch (IOException | RestClientException e) {
            fail(e);
        }
    }
}
