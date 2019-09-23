/* Licensed under Apache-2.0 */
package cricket.jmoore.security.basicauth;

import io.confluent.kafka.schemaregistry.client.security.basicauth.UrlBasicAuthCredentialProvider;

public class DestUrlBasicAuthCredentialProvider extends UrlBasicAuthCredentialProvider {
    @Override
    public String alias() {
        return "DEST_URL";
    }
}
