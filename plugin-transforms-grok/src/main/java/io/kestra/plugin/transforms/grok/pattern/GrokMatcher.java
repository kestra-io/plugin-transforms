package io.kestra.plugin.transforms.grok.pattern;

import io.kestra.plugin.transforms.grok.data.Type;
import org.jcodings.specific.UTF8Encoding;
import org.joni.Matcher;
import org.joni.NameEntry;
import org.joni.Option;
import org.joni.Regex;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public final class GrokMatcher {

    private final Map<String, GrokPattern> patternsByName;

    private final List<GrokPattern> patterns;

    private final String expression;

    private final Regex regex;

    private final List<GrokCaptureGroup> grokCaptureGroups;

    /**
     * Creates a new {@link GrokMatcher} instance.
     *
     * @param patterns    the list of patterns.
     * @param expression  the original expression.
     */
    GrokMatcher(final List<GrokPattern> patterns,
                final String expression) {
        Objects.requireNonNull(patterns, "pattern can't be null");
        Objects.requireNonNull(expression, "expression can't be null");
        this.patterns = patterns;
        this.expression = expression;
        this.patternsByName = patterns
            .stream()
            .collect(Collectors.toMap(GrokPattern::syntax, p -> p,  (p1, p2) -> p1.semantic() != null ? p1 : p2));
        byte[] bytes = expression.getBytes(StandardCharsets.UTF_8);
        regex = new Regex(bytes, 0, bytes.length, Option.NONE, UTF8Encoding.INSTANCE);

        grokCaptureGroups = new ArrayList<>();
        for (Iterator<NameEntry> entry = regex.namedBackrefIterator(); entry.hasNext();) {
            NameEntry nameEntry = entry.next();
            final String field = new String(
                nameEntry.name,
                nameEntry.nameP,
                nameEntry.nameEnd - nameEntry.nameP,
                StandardCharsets.UTF_8);
            final GrokPattern pattern = getGrokPattern(field);
            final Type type = pattern != null ? pattern.type() : Type.STRING;
            grokCaptureGroups.add(new GrokCaptureGroup(field, nameEntry.getBackRefs(), type));
        }
    }

    public GrokPattern getGrokPattern(final int i) {
        return patterns.get(i);
    }

    public GrokPattern getGrokPattern(final String name) {
        return patternsByName.get(name);
    }

    /**
     * Returns the compiled regex expression.
     */
    public Regex regex() {
        return regex;
    }

    /**
     * Returns the raw regex expression.
     */
    public String expression() {
        return expression;
    }

    /**
     *
     * @param bytes the text bytes to match.
     * @return      a {@code Map} that contains all named captured.
     */
    public Map<String, Object> captures(final byte[] bytes) {

        long now = System.currentTimeMillis();
        final var extractor = new GrokCaptureExtractor.MapGrokCaptureExtractor(grokCaptureGroups);

        final Matcher matcher = regex.matcher(bytes);
        int result = matcher.search(0, bytes.length, Option.DEFAULT);

        if (result == Matcher.FAILED) {
            return null;
        }
        if (result == Matcher.INTERRUPTED) {
            long interruptedAfterMs = System.currentTimeMillis() - now;
            throw new RuntimeException("Grok pattern matching was interrupted before completion (" + interruptedAfterMs + " ms)");
        }
        extractor.extract(bytes, matcher.getEagerRegion());

        return extractor.captured();
    }

    @Override
    public String toString() {
        return "GrokMatcher{" +
                "patterns=" + patterns +
                ", expression='" + expression + '\'' +
                '}';
    }
}
