/* Licensed under Apache-2.0 */
package cricket.jmoore.kafka.connect.transforms;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.avro.Schema;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.ResponseDefinitionTransformer;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

/**
 * <p>The schema registry mock implements a few basic HTTP endpoints that are used by the Avro serdes.</p>
 * In particular,
 * <ul>
 * <li>you can register a schema and</li>
 * <li>retrieve a schema by id.</li>
 * </ul>
 *
 * <p>Additionally, server-side mock can be toggled from its default authentication behavior (no authentication)
 * to a variant that requires basic HTTP Authentication using fixed credentials `username:password` by placing a
 * `@Tag(Constants.USE_BASIC_AUTH_SOURCE_TAG)` and/or `@Tag(Constants.USE_BASIC_AUTH_DESTR_TAG)` annotation after
 * @Test annotation of any basic HTTP authentication dependent test code.</p>
 *
 * <p>If you use the TestToplogy of the fluent Kafka Streams test, you don't have to interact with this class at
 * all.</p>
 *
 * <p>Without the test framework, you can use the mock as follows:</p>
 * <pre><code>
 * class SchemaRegistryMockTest {
 *     {@literal @RegisterExtension}
 *     final SchemaRegistryMock schemaRegistry = new SchemaRegistryMock();
 *
 *     {@literal @Test}
 *     void shouldRegisterKeySchema() throws IOException, RestClientException {
 *         final Schema keySchema = this.createSchema("key_schema");
 *         final int id = this.schemaRegistry.registerKeySchema("test-topic", keySchema);
 *
 *         final Schema retrievedSchema = this.schemaRegistry.getSchemaRegistryClient().getById(id);
 *         assertThat(retrievedSchema).isEqualTo(keySchema);
 *     }
 *
 *     {@literal @Test}
 *     {@literal @Tag(Constants.USE_BASIC_AUTH_SOURCE_TAG)}
 *     {@literal @Tag(Constants.USE_BASIC_AUTH_DEST_TAG)}
 *     void shouldUseBasicAuth() {
 *         final Map<String, Object> smtConfiguration = new HashMap<>();
 *         // ...
 *         smtConfiguration.put(ConfigName.SRC_BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
 *         smtConfiguration.put(ConfigName.SRC_USER_INFO, "username:password");
 *         smtConfiguration.put(ConfigName.SRC_BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
 *         smtConfiguration.put(ConfigName.SRC_USER_INFO, "username:password");
 *         // ...
 *         final SchemaRegistryTransfer<SourceRecord> smt = new SchemaRegistryTransfer<SourceRecord>();
 *         smt.configure(smtConfiguration);
 *         // ...
 *         smt.apply(...);
 *     }
 * }</code></pre>
 * <p>
 * To retrieve the url of the schema registry for a Kafka Streams config, please use {@link #getUrl()}
 */
public class SchemaRegistryMock implements BeforeEachCallback, AfterEachCallback {
    public enum Role {
        SOURCE,
        DESTINATION;
    }

    private static final String SCHEMA_REGISTRATION_PATTERN = "/subjects/[^/]+/versions";
    private static final String SCHEMA_BY_ID_PATTERN = "/schemas/ids/";
    private static final String CONFIG_PATTERN = "/config";
    private static final int IDENTITY_MAP_CAPACITY = 1000;
    private final ListVersionsHandler listVersionsHandler = new ListVersionsHandler();
    private final GetVersionHandler getVersionHandler = new GetVersionHandler();
    private final AutoRegistrationHandler autoRegistrationHandler = new AutoRegistrationHandler();
    private final GetConfigHandler getConfigHandler = new GetConfigHandler();
    private final WireMockServer mockSchemaRegistry = new WireMockServer(
            WireMockConfiguration.wireMockConfig().dynamicPort().extensions(
                    this.autoRegistrationHandler, this.listVersionsHandler, this.getVersionHandler,
                    this.getConfigHandler));
    private final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    private final String basicAuthTag;
    private Function<MappingBuilder, StubMapping> stubFor;

    private static final Logger log = LoggerFactory.getLogger(SchemaRegistryMock.class);

    public SchemaRegistryMock(Role role) {
        if (role == null) {
            throw new NullPointerException("Role must be either SOURCE or DESTINATION");
        }

        this.basicAuthTag = (role == Role.SOURCE) ? Constants.USE_BASIC_AUTH_SOURCE_TAG : Constants.USE_BASIC_AUTH_DEST_TAG; 
    }

    @Override
    public void afterEach(final ExtensionContext context) {
        this.mockSchemaRegistry.stop();
    }

    @Override
    public void beforeEach(final ExtensionContext context) {
        if (context.getTags().contains(this.basicAuthTag)) {
            String[] userPass = Constants.HTTP_AUTH_CREDENTIALS_FIXTURE.split(":");
            this.stubFor = (MappingBuilder mappingBuilder) -> this.mockSchemaRegistry.stubFor(
                    mappingBuilder.withBasicAuth(userPass[0], userPass[1]));
        } else {
            this.stubFor = (MappingBuilder mappingBuilder) -> this.mockSchemaRegistry.stubFor(mappingBuilder);
        }

        this.mockSchemaRegistry.start();
        this.stubFor.apply(WireMock.get(WireMock.urlPathMatching(SCHEMA_REGISTRATION_PATTERN))
                .willReturn(WireMock.aResponse().withTransformers(this.listVersionsHandler.getName())));
        this.stubFor.apply(WireMock.post(WireMock.urlPathMatching(SCHEMA_REGISTRATION_PATTERN))
                .willReturn(WireMock.aResponse().withTransformers(this.autoRegistrationHandler.getName())));
        this.stubFor.apply(WireMock.get(WireMock.urlPathMatching(SCHEMA_REGISTRATION_PATTERN + "/(?:latest|\\d+)"))
                .willReturn(WireMock.aResponse().withTransformers(this.getVersionHandler.getName())));
        this.stubFor.apply(WireMock.get(WireMock.urlPathMatching(CONFIG_PATTERN))
                .willReturn(WireMock.aResponse().withTransformers(this.getConfigHandler.getName())));
        this.stubFor.apply(WireMock.get(WireMock.urlPathMatching(SCHEMA_BY_ID_PATTERN + "\\d+"))
                .willReturn(WireMock.aResponse().withStatus(HTTP_NOT_FOUND)));
    }

    public int registerSchema(final String topic, boolean isKey, final Schema schema) {
        return this.registerSchema(topic, isKey, schema, new TopicNameStrategy());
    }

    public int registerSchema(final String topic, boolean isKey, final Schema schema, SubjectNameStrategy<Schema> strategy) {
        return this.register(strategy.subjectName(topic, isKey, schema), schema);
    }

    private int register(final String subject, final Schema schema) {
        try {
            final int id = this.schemaRegistryClient.register(subject, schema);
            this.stubFor.apply(WireMock.get(WireMock.urlEqualTo(SCHEMA_BY_ID_PATTERN + id))
                    .willReturn(ResponseDefinitionBuilder.okForJson(new SchemaString(schema.toString()))));
            log.debug("Registered schema {}", id);
            return id;
        } catch (final IOException | RestClientException e) {
            throw new IllegalStateException("Internal error in mock schema registry client", e);
        }
    }

    private List<Integer> listVersions(String subject) {
        log.debug("Listing all versions for subject {}", subject);
        try {
            return this.schemaRegistryClient.getAllVersions(subject);
        } catch (IOException | RestClientException e) {
            throw new IllegalStateException("Internal error in mock schema registry client", e);
        }
    }

    private SchemaMetadata getSubjectVersion(String subject, Object version) {
        log.debug("Requesting version {} for subject {}", version, subject);
        try {
            if (version instanceof String && version.equals("latest")) {
                return this.schemaRegistryClient.getLatestSchemaMetadata(subject);
            } else if (version instanceof Number) {
                return this.schemaRegistryClient.getSchemaMetadata(subject, ((Number) version).intValue());
            } else {
                throw new IllegalArgumentException("Only 'latest' or integer versions are allowed");
            }
        } catch (IOException | RestClientException e) {
            throw new IllegalStateException("Internal error in mock schema registry client", e);
        }
    }

    private String getCompatibility(String subject) {
        if (subject == null) {
            log.debug("Requesting registry base compatibility");
        } else {
            log.debug("Requesting compatibility for subject {}", subject);
        }
        try {
            return this.schemaRegistryClient.getCompatibility(subject);
        } catch (IOException | RestClientException e) {
            throw new IllegalStateException("Internal error in mock schema registry client", e);
        }
    }

    public SchemaRegistryClient getSchemaRegistryClient() {
        return new CachedSchemaRegistryClient(this.getUrl(), IDENTITY_MAP_CAPACITY);
    }

    public String getUrl() {
        return "http://localhost:" + this.mockSchemaRegistry.port();
    }

    private abstract class SubjectsVersioHandler extends ResponseDefinitionTransformer {
        // Expected url pattern /subjects/.*-value/versions
        protected final Splitter urlSplitter = Splitter.on('/').omitEmptyStrings();

        protected String getSubject(Request request) {
            return Iterables.get(this.urlSplitter.split(request.getUrl()), 1);
        }

        @Override
        public boolean applyGlobally() {
            return false;
        }
    }

    private class AutoRegistrationHandler extends SubjectsVersioHandler {

        @Override
        public ResponseDefinition transform(final Request request, final ResponseDefinition responseDefinition,
                                            final FileSource files, final Parameters parameters) {
            try {
                final int id = SchemaRegistryMock.this.register(getSubject(request),
                        new Schema.Parser()
                                .parse(RegisterSchemaRequest.fromJson(request.getBodyAsString()).getSchema()));
                final RegisterSchemaResponse registerSchemaResponse = new RegisterSchemaResponse();
                registerSchemaResponse.setId(id);
                return ResponseDefinitionBuilder.jsonResponse(registerSchemaResponse);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Cannot parse schema registration request", e);
            }
        }

        @Override
        public String getName() {
            return AutoRegistrationHandler.class.getSimpleName();
        }
    }

    private class ListVersionsHandler extends SubjectsVersioHandler {

        @Override
        public ResponseDefinition transform(final Request request, final ResponseDefinition responseDefinition,
                                            final FileSource files, final Parameters parameters) {
            final List<Integer> versions = SchemaRegistryMock.this.listVersions(getSubject(request));
            log.debug("Got versions {}", versions);
            return ResponseDefinitionBuilder.jsonResponse(versions);
        }

        @Override
        public String getName() {
            return ListVersionsHandler.class.getSimpleName();
        }
    }

    private class GetVersionHandler extends SubjectsVersioHandler {

        @Override
        public ResponseDefinition transform(final Request request, final ResponseDefinition responseDefinition,
                                            final FileSource files, final Parameters parameters) {
            String versionStr = Iterables.get(this.urlSplitter.split(request.getUrl()), 3);
            SchemaMetadata metadata;
            if (versionStr.equals("latest")) {
                metadata = SchemaRegistryMock.this.getSubjectVersion(getSubject(request), versionStr);
            } else {
                int version = Integer.parseInt(versionStr);
                metadata = SchemaRegistryMock.this.getSubjectVersion(getSubject(request), version);
            }
            return ResponseDefinitionBuilder.jsonResponse(metadata);
        }

        @Override
        public String getName() {
            return GetVersionHandler.class.getSimpleName();
        }
    }

    private class GetConfigHandler extends SubjectsVersioHandler {

        @Override
        protected String getSubject(Request request) {
            List<String> parts =
                    StreamSupport.stream(this.urlSplitter.split(request.getUrl()).spliterator(), false)
                            .collect(Collectors.toList());

            // return null when this is just /config
            return parts.size() < 2 ? null : parts.get(1);
        }

        @Override
        public ResponseDefinition transform(final Request request, final ResponseDefinition responseDefinition,
                                            final FileSource files, final Parameters parameters) {
            Config config = new Config(SchemaRegistryMock.this.getCompatibility(getSubject(request)));
            return ResponseDefinitionBuilder.jsonResponse(config);
        }

        @Override
        public String getName() {
            return GetConfigHandler.class.getSimpleName();
        }
    }

}
