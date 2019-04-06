/* Licensed under Apache-2.0 */
package cricket.jmoore.kafka.connect.transforms;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

public class SchemaTest {

    @Test
    public void testBytesSchema() {
        assertTrue(ConnectSchemaUtil.isBytesSchema(Schema.BYTES_SCHEMA));
        assertTrue(ConnectSchemaUtil.isBytesSchema(Schema.OPTIONAL_BYTES_SCHEMA));
    }

    @Test
    public void testNullIsNotBytes() {
        assertFalse(ConnectSchemaUtil.isBytesSchema(null));
    }

    @Test
    public void testNonByteTypeSchemas() {
        Schema[] schemas = new Schema[]{
                // Boolean
                Schema.BOOLEAN_SCHEMA,
                Schema.OPTIONAL_BOOLEAN_SCHEMA,
                // Integers
                Schema.INT8_SCHEMA,
                Schema.INT16_SCHEMA,
                Schema.INT32_SCHEMA,
                Schema.INT64_SCHEMA,
                Schema.OPTIONAL_INT8_SCHEMA,
                Schema.OPTIONAL_INT16_SCHEMA,
                Schema.OPTIONAL_INT32_SCHEMA,
                Schema.OPTIONAL_INT64_SCHEMA,
                // Floats
                Schema.FLOAT32_SCHEMA,
                Schema.FLOAT64_SCHEMA,
                Schema.OPTIONAL_FLOAT32_SCHEMA,
                Schema.OPTIONAL_FLOAT64_SCHEMA,
                // String
                Schema.STRING_SCHEMA,
                Schema.OPTIONAL_STRING_SCHEMA,
                // Struct with a field of bytes
                SchemaBuilder.struct().name("record").
                        field("foo", Schema.BYTES_SCHEMA)
                        .build(),
                SchemaBuilder.struct().name("record").
                        field("foo", Schema.OPTIONAL_BYTES_SCHEMA)
                        .build(),
                // map<bytes, bytes>
                SchemaBuilder.map(Schema.BYTES_SCHEMA, Schema.OPTIONAL_BYTES_SCHEMA).build(),
                // array<bytes>
                SchemaBuilder.array(Schema.OPTIONAL_BYTES_SCHEMA).build()
        };

        for (Schema s : schemas) {
            assertFalse(ConnectSchemaUtil.isBytesSchema(s));
        }
    }
}
