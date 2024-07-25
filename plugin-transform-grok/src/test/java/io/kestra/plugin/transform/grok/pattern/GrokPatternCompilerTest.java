package io.kestra.plugin.transform.grok.pattern;


import io.kestra.plugin.transform.grok.data.Type;
import io.kestra.plugin.transform.grok.pattern.GrokMatcher;
import io.kestra.plugin.transform.grok.pattern.GrokPatternCompiler;
import io.kestra.plugin.transform.grok.pattern.GrokPatternResolver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GrokPatternCompilerTest {

    private final GrokPatternResolver resolver = new GrokPatternResolver();
    private final GrokPatternCompiler compiler = new GrokPatternCompiler(new GrokPatternResolver(), false);


    @Test
    public void shouldCompileAllDefinitions() {
        Map<String, String> definitions = resolver.definitions();
        List<Map.Entry<String, String>> errors = new ArrayList<>(definitions.size());
        for (Map.Entry<String, String> definition : definitions.entrySet()) {
            try {
               compiler.compile("%{" + definition.getKey() + "}");
            } catch (Exception e) {
                errors.add(definition);
            }
        }
        Assertions.assertTrue(errors.isEmpty(), "Failed to compile '[" +  errors.size() + "]' definitions: " + errors.stream().map(Map.Entry::getKey).collect(Collectors.toSet()));
    }

    @Test
    public void shouldCompileMatcherGivenSingleGrokPattern() {
        final GrokMatcher matcher = compiler.compile("%{ISO8601_TIMEZONE}");
        Assertions.assertNotNull(matcher);
        Assertions.assertEquals("ISO8601_TIMEZONE", matcher.getGrokPattern(0).syntax());
        Assertions.assertEquals("(?<ISO8601_TIMEZONE>(?:Z|[+-](?<HOUR>(?:2[0123]|[01]?[0-9]))(?::?(?<MINUTE>(?:[0-5][0-9])))))", matcher.expression());
    }

    @Test
    public void shouldCompileMatcherGivenMultipleGrokPattern() {
        final GrokMatcher matcher = compiler.compile("%{ISO8601_TIMEZONE} %{LOGLEVEL} %{GREEDYDATA}");
        Assertions.assertNotNull(matcher);
        Assertions.assertNotNull(matcher.getGrokPattern("ISO8601_TIMEZONE"));
        Assertions.assertNotNull(matcher.getGrokPattern("LOGLEVEL"));
        Assertions.assertNotNull(matcher.getGrokPattern("GREEDYDATA"));
        Assertions.assertEquals("(?<ISO8601_TIMEZONE>(?:Z|[+-](?<HOUR>(?:2[0123]|[01]?[0-9]))(?::?(?<MINUTE>(?:[0-5][0-9]))))) (?<LOGLEVEL>([Aa]lert|ALERT|[Tt]race|TRACE|[Dd]ebug|DEBUG|[Nn]otice|NOTICE|[Ii]nfo?(?:rmation)?|INFO?(?:RMATION)?|[Ww]arn?(?:ing)?|WARN?(?:ING)?|[Ee]rr?(?:or)?|ERR?(?:OR)?|[Cc]rit?(?:ical)?|CRIT?(?:ICAL)?|[Ff]atal|FATAL|[Ss]evere|SEVERE|EMERG(?:ENCY)?|[Ee]merg(?:ency)?)) (?<GREEDYDATA>.*)", matcher.expression());
    }

    @Test
    public void shouldCompileMatcherGivenMultipleGrokPatternWithSemantic() {
        final GrokMatcher matcher = compiler.compile("%{ISO8601_TIMEZONE:timezone}");
        Assertions.assertNotNull(matcher);
        Assertions.assertEquals("ISO8601_TIMEZONE", matcher.getGrokPattern(0).syntax());
        Assertions.assertEquals("timezone", matcher.getGrokPattern(0).semantic());
        Assertions.assertEquals("(?<timezone>(?:Z|[+-](?<HOUR>(?:2[0123]|[01]?[0-9]))(?::?(?<MINUTE>(?:[0-5][0-9])))))", matcher.expression());
    }

    @Test
    public void shouldCompileMatcherGivenMultipleGrokPatternWithSemanticAndType() {
        final GrokMatcher matcher = compiler.compile("%{ISO8601_TIMEZONE:timezone:int}");
        Assertions.assertNotNull(matcher);
        Assertions.assertEquals("ISO8601_TIMEZONE", matcher.getGrokPattern(0).syntax());
        Assertions.assertEquals("timezone", matcher.getGrokPattern(0).semantic());
        Assertions.assertEquals(Type.INT, matcher.getGrokPattern(0).type());
        Assertions.assertEquals("(?<timezone>(?:Z|[+-](?<HOUR>(?:2[0123]|[01]?[0-9]))(?::?(?<MINUTE>(?:[0-5][0-9])))))", matcher.expression());
    }

    @Test
    public void shouldCompileMatcherGivenCustomGrokPattern() {
        final GrokMatcher matcher = compiler.compile("(?<email>^[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,6}$)");
        Assertions.assertNotNull(matcher);
        Assertions.assertEquals("(?<email>^[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,6}$)", matcher.expression());
    }
}