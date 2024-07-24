package io.kestra.plugin.transforms.grok.pattern;

import io.kestra.plugin.transforms.grok.data.Type;

/**
 * GrokPattern.
 *
 * @param syntax   the grok pattern syntax.
 * @param semantic the grok pattern semantic.
 * @param type     the grok pattern type.
 */
public record GrokPattern(
    String syntax,
    String semantic,
    Type type
) {

    /**
     * Creates a new {@link GrokPattern} instance.
     *
     * @param syntax   the grok pattern syntax.
     * @param semantic the grok pattern semantic.
     * @param type     the grok pattern type.
     */
    public static GrokPattern of(final String syntax, final String semantic, final String type) {
        return new GrokPattern(
            syntax,
            semantic,
            type != null ? Type.getForNameIgnoreCase(type.toUpperCase(), Type.STRING) : Type.STRING
        );
    }
}
