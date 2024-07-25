package io.kestra.plugin.transform.grok.pattern;


import io.kestra.plugin.transform.grok.pattern.GrokPatternResolver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GrokPatternResolverTest {

    @Test
    public void shouldLoadAllGrokPatternsFromClasspath() {
        GrokPatternResolver resolver = new GrokPatternResolver();
        resolver.print();
        Assertions.assertFalse(resolver.isEmpty());
    }

    @Test
    public void shouldStandardResolveGrokPattern() {
        GrokPatternResolver resolver = new GrokPatternResolver();
        String resolve = resolver.resolve("SYSLOGFACILITY");
        Assertions.assertEquals("<%{NONNEGINT:[log][syslog][facility][code]:int}.%{NONNEGINT:[log][syslog][priority]:int}>", resolve);
    }

}