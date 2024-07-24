package io.kestra.plugin.transforms.grok.pattern;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

class GrokMatcherTest {
    private GrokPatternCompiler compiler;

    @BeforeEach
    public void setUp() {
        compiler = new GrokPatternCompiler(new GrokPatternResolver(), false);
    }

    @Test
    public void shouldParseGivenSimpleGrokPattern() {
        GrokPatternCompiler compiler = new GrokPatternCompiler(new GrokPatternResolver(), false);
        final GrokMatcher matcher = compiler.compile("%{EMAILADDRESS}");
        final Map<String, Object> captured = matcher.captures("test@kafka.org".getBytes(StandardCharsets.UTF_8));
        Assertions.assertEquals("kafka.org", captured.get("HOSTNAME"));
        Assertions.assertEquals("test@kafka.org", captured.get("EMAILADDRESS"));
        Assertions.assertEquals("test", captured.get("EMAILLOCALPART"));
    }

    @Test
    public void shouldParseGivenCustomGrokPattern() {
        final GrokMatcher matcher = compiler.compile("(?<EMAILADDRESS>(?<EMAILLOCALPART>[a-zA-Z][a-zA-Z0-9_.+-=:]+)@(?<HOSTNAME>\\b(?:[0-9A-Za-z][0-9A-Za-z-]{0,62})(?:\\.(?:[0-9A-Za-z][0-9A-Za-z-]{0,62}))*(\\.?|\\b)))");
        final Map<String, Object> captured = matcher.captures("test@kestra.io".getBytes(StandardCharsets.UTF_8));
        Assertions.assertEquals("kestra.io", captured.get("HOSTNAME"));
        Assertions.assertEquals("test@kestra.io", captured.get("EMAILADDRESS"));
        Assertions.assertEquals("test", captured.get("EMAILLOCALPART"));
    }
}