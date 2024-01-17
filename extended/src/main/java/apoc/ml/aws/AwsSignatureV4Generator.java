package apoc.ml.aws;

import org.apache.commons.lang3.tuple.Pair;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;


public class AwsSignatureV4Generator {

    public static final String AUTHORIZATION_KEY = "Authorization";

    /**
     * Generates signing headers for HTTP request in accordance with Amazon AWS API Signature version 4 process.
     * <p>
     * Following steps outlined here: <a href="https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html">docs.aws.amazon.com</a>
     * <p>
     * @param conf - The {@link AWSConfig config}
     * @param bodyString - The HTTP body
     */
    public static void calculateAuthorizationHeaders(
            AWSConfig conf,
            String bodyString,
            Map<String, Object> headers,
            String awsServiceName
    ) throws MalformedURLException {
        
        // skip if "Authorization" has already been valued
        if (headers.containsKey(AUTHORIZATION_KEY)) {
            return;
        }

        byte[] body = getBytes(bodyString);
        String isoDateTime = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").format(ZonedDateTime.now(ZoneOffset.UTC));

        URL url = new URL(conf.getEndpoint());

        String host = url.getHost();
        String path = url.getPath();
        String query = url.getQuery();

        String bodySha256 = hex(toSha256(body));
        String isoDateOnly = isoDateTime.substring(0, 8);

        headers.put("Host", host);
        headers.put("X-Amz-Date", isoDateTime);

        Pair<String, String> pairSignedHeaderAndCanonicalHash = createCanonicalRequest(conf.getMethod(), headers, path, query, bodySha256);

        Pair<String, String> pairCredentialAndStringSign = createStringToSign(conf.getRegion(), isoDateTime, isoDateOnly, pairSignedHeaderAndCanonicalHash, awsServiceName);

        String signature = calculateSignature(conf.getSecretKey(), conf.getRegion(), isoDateOnly, pairCredentialAndStringSign.getRight(), awsServiceName);

        createAuthorizationHeader(conf, headers, pairSignedHeaderAndCanonicalHash, pairCredentialAndStringSign, signature);
    }

    private static byte[] getBytes(String bodyString) {
        if (bodyString == null) {
            bodyString = "";
        }
        return bodyString.getBytes();
    }

    private static void createAuthorizationHeader(AWSConfig conf, Map<String, Object> headers, Pair<String, String> pairSignedHeaderAndCanonicalHash, Pair<String, String> pairCredentialAndStringSign, String signature) {
        String authStringParameter = "AWS4-HMAC-SHA256 Credential=" + conf.getKeyId() + "/" + pairCredentialAndStringSign.getLeft()
                                     + ", SignedHeaders=" + pairSignedHeaderAndCanonicalHash.getLeft()
                                     + ", Signature=" + signature;

        headers.put(AUTHORIZATION_KEY, authStringParameter);
    }

    /**
     * Based on <a href="https://docs.aws.amazon.com/general/latest/gr/sigv4-create-string-to-sign.html">sigv4-create-string-to-sign</a>
     */
    private static Pair<String, String> createStringToSign(String awsRegion, String isoDateTime, String isoJustDate, Pair<String, String> pairSignedHeaderCanonicalHash, String awsServiceName) {
        List<String> stringToSignLines = new ArrayList<>();
        stringToSignLines.add("AWS4-HMAC-SHA256");
        stringToSignLines.add(isoDateTime);
        String credentialScope = isoJustDate + "/" + awsRegion + "/" + awsServiceName + "/aws4_request";
        stringToSignLines.add(credentialScope);
        stringToSignLines.add(pairSignedHeaderCanonicalHash.getRight());
        String stringToSign = String.join("\n", stringToSignLines);
        return Pair.of(credentialScope, stringToSign);
    }

    /**
     * Based on <a href="https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html">sigv4-create-canonical-request</a>
     */
    private static Pair<String, String> createCanonicalRequest(String method, Map<String, Object> headers, String path, String query, String bodySha256) {
        List<String> canonicalRequestLines = new ArrayList<>();
        canonicalRequestLines.add(method);
        canonicalRequestLines.add(path);
        canonicalRequestLines.add(query);
        List<String> hashedHeaders = new ArrayList<>();
        List<String> headerKeysSorted = headers.keySet().stream().sorted(Comparator.comparing(e -> e.toLowerCase(Locale.US))).toList();
        for (String key : headerKeysSorted) {
            hashedHeaders.add(key.toLowerCase(Locale.US));
            canonicalRequestLines.add(key.toLowerCase(Locale.US) + ":" + normalizeSpaces((String) headers.get(key)));
        }
        canonicalRequestLines.add(null); // new line required after headers
        String signedHeaders = String.join(";", hashedHeaders);
        canonicalRequestLines.add(signedHeaders);
        canonicalRequestLines.add(bodySha256);
        String canonicalRequestBody = canonicalRequestLines.stream().map(line -> line == null ? "" : line).collect(Collectors.joining("\n"));
        String canonicalRequestHash = hex(toSha256(canonicalRequestBody.getBytes(StandardCharsets.UTF_8)));
        return Pair.of(signedHeaders, canonicalRequestHash);
    }

    /**
     * Based on <a href="https://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html">sigv4-calculate-signature</a>
     */
    private static String calculateSignature(String awsSecret, String awsRegion, String isoJustDate, String stringToSign, String awsServiceName) {
        byte[] kDate = toHmac(("AWS4" + awsSecret).getBytes(StandardCharsets.UTF_8), isoJustDate);
        byte[] kRegion = toHmac(kDate, awsRegion);
        byte[] kService = toHmac(kRegion, awsServiceName);
        byte[] kSigning = toHmac(kService, "aws4_request");
        return hex(toHmac(kSigning, stringToSign));
    }

    private static String normalizeSpaces(String value) {
        return value.replaceAll("\\s+", " ").trim();
    }

    public static String hex(byte[] a) {
        StringBuilder sb = new StringBuilder(a.length * 2);
        for(byte b: a) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private static byte[] toSha256(byte[] bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(bytes);
            return digest.digest();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] toHmac(byte[] key, String msg) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(msg.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}