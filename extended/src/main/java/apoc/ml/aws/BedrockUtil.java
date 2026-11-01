package apoc.ml.aws;

import java.util.regex.Pattern;


public class BedrockUtil {
    public static final Pattern SERVICE_HOST_PATTERN = Pattern.compile("bedrock(-[a-z]+)*\\.[a-z0-9-]+\\.amazonaws\\.com");
    public static final String JURASSIC_2_ULTRA = "ai21.j2-ultra-v1";
    public static final String TITAN_EMBED_TEXT = "amazon.titan-embed-text-v1";
    public static final String STABILITY_STABLE_DIFFUSION_XL = "stability.stable-diffusion-xl-v0";
}
