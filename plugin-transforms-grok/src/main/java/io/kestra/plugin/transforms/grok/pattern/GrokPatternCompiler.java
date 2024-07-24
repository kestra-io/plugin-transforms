package io.kestra.plugin.transforms.grok.pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class GrokPatternCompiler {

    private static final Logger LOG = LoggerFactory.getLogger(GrokPatternCompiler.class);

    private static final String SYNTAX_FIELD = "syntax";
    private static final String SEMANTIC_FIELD = "semantic";
    private static final String TYPE_FIELD = "type";

    private static final String REGEX = "(?:%\\{(?<syntax>[A-Z0-9_]+)(?:\\:(?<semantic>[a-zA-Z0-9_\\\\-]+))?(?:\\:(?<type>[a-zA-Z0-9_\\\\-]+))?\\})";
    private static final Pattern PATTERN = Pattern.compile(REGEX);

    private final GrokPatternResolver resolver;

    private final boolean namedCapturesOnly;

    /**
     * Creates a new {@link GrokPatternCompiler} instance.
     *
     * @param resolver          the grok pattern resolver.
     * @param namedCapturesOnly is only named pattern should be captured.
     */
    public GrokPatternCompiler(final GrokPatternResolver resolver,
                               final boolean namedCapturesOnly) {
        Objects.requireNonNull(resolver, "resolver can't be null");
        this.resolver = resolver;
        this.namedCapturesOnly = namedCapturesOnly;
    }

    public GrokMatcher compile(final String expression) {
        Objects.requireNonNull(expression, "expression can't be null");
        LOG.trace("Starting to compile grok matcher expression : {}", expression);
        ArrayList<GrokPattern> patterns = new ArrayList<>();
        final String regex = compileRegex(expression, patterns);
        LOG.trace("Grok expression compiled to regex : {}", regex);
        return new GrokMatcher(patterns, regex);
    }

    private String compileRegex(final String expression, final List<GrokPattern> patterns) {
        Matcher matcher = PATTERN.matcher(expression);
        final StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            GrokPattern grok = GrokPattern.of(
                matcher.group(SYNTAX_FIELD),
                matcher.group(SEMANTIC_FIELD),
                matcher.group(TYPE_FIELD)
            );

            patterns.add(grok);

            final String resolved = resolver.resolve(grok.syntax());
            String replacement = compileRegex(resolved, patterns);
            if (grok.semantic() != null) {
                replacement = capture(replacement, grok.semantic());
            } else if (!namedCapturesOnly) {
                replacement = capture(replacement, grok.syntax());
            }

            // Properly escape $ characters
            // Illegal group reference exception can arise when the replacement string for
            // Matcher.appendReplacement contains illegal references to capture groups,
            // which could occur when there are unresolved or incorrectly escaped group references.

            // This often happens with the $ character in regex, which signifies a group reference.
            replacement = replacement.replace("\\", "\\\\").replace("$", "\\$");

            try {
                matcher.appendReplacement(sb, replacement);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Failed to : " + replacement, e);
            }
        }
        // Copy the remainder of the input sequence.
        matcher.appendTail(sb);
        return sb.toString();
    }

    private String capture(final String expression, final String name) {
        return "(?<" + name + ">" + expression + ")";
    }
}
