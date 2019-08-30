/* Licensed under Apache-2.0 */
package cricket.jmoore.security.basicauth;

import io.confluent.kafka.schemaregistry.client.security.basicauth.SaslBasicAuthCredentialProvider;

public class SrcSaslBasicAuthCredentialProvider extends SaslBasicAuthCredentialProvider {
    @Override
    public String alias() {
        return "SRC_SASL_INHERIT";
    }
}
