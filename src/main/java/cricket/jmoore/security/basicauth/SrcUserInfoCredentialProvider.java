/* Licensed under Apache-2.0 */
package cricket.jmoore.security.basicauth;

import io.confluent.kafka.schemaregistry.client.security.basicauth.UserInfoCredentialProvider;

public class SrcUserInfoCredentialProvider extends UserInfoCredentialProvider
{
    @Override
    public String alias() {
        return "SRC_USER_INFO";
    }
}
