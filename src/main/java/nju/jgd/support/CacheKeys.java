package nju.jgd.support;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.Normalizer;
import java.util.HexFormat;
import java.util.Locale;
import java.util.regex.Pattern;

/** Normalized keys used by the exact-match response cache and memory paths. */
public final class CacheKeys {

    private static final Pattern SAFE_SEGMENT = Pattern.compile("[A-Za-z0-9_-]{1,64}");

    private CacheKeys() {}

    /**
     * Builds a deterministic cache key.
     *
     * @param message normalized user message
     * @param model configured model name
     * @param promptVersion prompt contract version
     * @return SHA-256 hex key
     */
    public static String cacheKey(String message, String model, String promptVersion) {
        String normalized = normalizeMessage(message);
        return sha256Hex(model + "\n" + promptVersion + "\n" + normalized);
    }

    /** Normalizes whitespace, Unicode width, and casing before cache lookup. */
    public static String normalizeMessage(String message) {
        if (message == null) {
            return "";
        }
        return Normalizer.normalize(message, Normalizer.Form.NFKC)
                .trim()
                .replaceAll("\\s+", " ")
                .toLowerCase(Locale.ROOT);
    }

    /** Converts an external identifier into a safe Flink memory path segment. */
    public static String safeSegment(String value) {
        if (value != null && SAFE_SEGMENT.matcher(value).matches()) {
            return value;
        }
        return "h_" + sha256Hex(String.valueOf(value)).substring(0, 24);
    }

    /** Returns a lowercase SHA-256 hex digest. */
    public static String sha256Hex(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of()
                    .formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is not available", exception);
        }
    }
}
