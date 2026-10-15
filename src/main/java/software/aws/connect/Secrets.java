package software.aws.connect;

import java.io.IOException;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Reads JSON secrets from AWS Secrets Manager. Never logs secret values. */
final class Secrets {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Secrets() {
    }

    static JsonNode load(String region, String secretId) throws IOException {
        AWSSecretsManager client = AWSSecretsManagerClientBuilder.standard().withRegion(region).build();
        try {
            String value = client.getSecretValue(new GetSecretValueRequest().withSecretId(secretId)).getSecretString();
            if (value == null || value.isEmpty()) {
                throw new IOException("Secret " + secretId + " has no string value");
            }
            return MAPPER.readTree(value);
        } finally {
            client.shutdown();
        }
    }

    static String require(JsonNode secret, String key, String secretId) {
        String value = secret.path(key).asText("");
        if (value.isEmpty()) {
            throw new IllegalStateException("Secret " + secretId + " is missing key " + key);
        }
        return value;
    }
}
