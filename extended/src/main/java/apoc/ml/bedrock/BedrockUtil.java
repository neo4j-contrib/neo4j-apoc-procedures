package apoc.ml.bedrock;

import uk.co.lucasweb.aws.v4.signer.HttpRequest;
import uk.co.lucasweb.aws.v4.signer.Signer;
import uk.co.lucasweb.aws.v4.signer.credentials.AwsCredentials;
import uk.co.lucasweb.aws.v4.signer.hash.Sha256;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;


public class BedrockUtil {
    public static final String SERVICE_NAME = "bedrock";
    
    public static final String AUTHORIZATION = "Authorization";
    public static final String X_AMZ_DATE = "X-Amz-Date";
    
    public static final String JURASSIC_2_ULTRA = "ai21.j2-ultra-v1";
    public static final String TITAN_EMBED_TEXT = "amazon.titan-embed-text-v1";
    public static final String ANTHROPIC_CLAUDE_V2 = "anthropic.claude-v2";
    public static final String STABILITY_STABLE_DIFFUSION_XL = "stability.stable-diffusion-xl-v0";
    
    /**
     * Leveraging <a href="https://github.com/lucasweb78/aws-v4-signer-java/">.aws-v4-signer-java</a>
     */
    public static void calculateAuthorizationHeaders(
            BedrockConfig conf,
            String body) {
        try {
            Map<String, Object> headers = conf.getHeaders();
            if (headers.containsKey(AUTHORIZATION)) {
                return;
            }
            
            URL endpointUrl = new URL(conf.getEndpoint());
            String isoDateTime = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").format(ZonedDateTime.now(ZoneOffset.UTC));
            HttpRequest request = new HttpRequest(conf.getMethod(), endpointUrl.toURI());

            Signer.Builder builder = Signer.builder()
                    .awsCredentials(new AwsCredentials(conf.getKeyId(), conf.getSecretKey()))
                    .header("Host", endpointUrl.getHost())
                    .header(X_AMZ_DATE, isoDateTime);
            
            // create signature for `bedrock`
            String signature = builder
                    .build(request, SERVICE_NAME, Sha256.get(body, StandardCharsets.UTF_8))
                    .getSignature();

            headers.put(AUTHORIZATION, signature);
            headers.put(X_AMZ_DATE, isoDateTime);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
