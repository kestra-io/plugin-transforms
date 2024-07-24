package io.kestra.plugin.transforms.grok;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.transforms.grok.pattern.GrokMatcher;
import io.kestra.plugin.transforms.grok.pattern.GrokPatternCompiler;
import io.kestra.plugin.transforms.grok.pattern.GrokPatternResolver;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Parse arbitrary text and structure it using Grok expressions.",
    description = """
        The `GrokTransform` task is similar to the famous Logstash Grok filter from the ELK stack.
        It is particularly useful for transforming unstructured data such as logs into a structured, indexable, and queryable data structure.
        
        The `GrokTransform` ships with all the default patterns as defined You can find them here: .
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Consume, parse, and structure logs events from Kafka topic.",
            full = true,
            code = """
                id: kafka
                namespace: myteam

                tasks:
                - id: grok
                  type: io.kestra.plugin.transforms.grok.GrokTransform
                  pattern: "%{TIMESTAMP_ISO8601:logdate} %{LOGLEVEL:loglevel} %{GREEDYDATA:message}"
                  from: "{{ trigger.value }}"
                  
                - id: log_on_warn
                  type: io.kestra.plugin.core.flow.If
                  condition: "{{ grok.value['LOGLEVEL'] == 'ERROR' }}"
                  then:
                    - id: when_true
                      type: io.kestra.plugin.core.log.Log
                      message: "{{ grok.value }}"
                          
                triggers:
                - id: realtime_trigger
                  type: io.kestra.plugin.kafka.RealtimeTrigger
                  topic: test_kestra
                  properties:
                    bootstrap.servers: localhost:9092
                  serdeProperties:
                    schema.registry.url: http://localhost:8085
                    keyDeserializer: STRING
                    valueDeserializer: STRING
                  groupId: kafkaConsumerGroupId   
                """
        )
    }
)
public class GrokTransform extends Task implements RunnableTask<Output> {

    private static final ObjectMapper ION_OBJECT_MAPPER = JacksonMapper.ofIon();

    @Schema(
        title = "The string object to transform.",
        description = "Must be a valid JSON string or a `kestra://` internal storage URI."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;

    @PluginProperty
    @Schema(title = "A Grok pattern to match.")
    private String pattern;

    @PluginProperty
    @Schema(title = "A list of Grok patterns to match.")
    private List<String> patterns;

    @PluginProperty
    @Schema(
        title = "List of user-defined pattern directories",
        description = "Directories must be paths relative to the working directory."
    )
    @NotNull
    private List<String> patternsDir;

    @PluginProperty
    @Schema(
        title = "Custom pattern definitions",
        description = "A map of pattern-name and pattern pairs defining custom patterns to be used by the current tasks. Patterns matching existing names will override the pre-existing definition. "
    )
    private Map<String, String> patternDefinitions;

    @PluginProperty
    @Schema(title = "If `true`, only store named captures from grok.")
    @Builder.Default
    private boolean namedCapturesOnly = true;

    @PluginProperty
    @Schema(
        title = "If `true`, break on first match.",
        description = "The first successful match by grok will result in the task being finished. Set to `false` if you want the task to try all configured patterns."
    )
    @Builder.Default
    private boolean breakOnFirstMatch = true;

    @Getter(AccessLevel.PRIVATE)
    private RunContext runContext;

    /**
     * {@inheritDoc}
     **/
    @Override
    public Output run(RunContext runContext) throws Exception {

        this.runContext = runContext;

        final byte[] bytes = runContext.render(this.from).getBytes(StandardCharsets.UTF_8);

        GrokPatternCompiler compiler = new GrokPatternCompiler(
            new GrokPatternResolver(
                runContext.logger(),
                patternDefinitions(),
                patternsDir()
            ),
            namedCapturesOnly
        );

        // compile all patterns
        List<GrokMatcher> matchPatterns = patterns()
            .stream()
            .map(compiler::compile)
            .toList();

        // match patterns
        final List<Map<String, Object>> allNamedCaptured = new ArrayList<>(matchPatterns.size());
        for (GrokMatcher matcher : matchPatterns) {
            final Map<String, Object> captured = matcher.captures(bytes);
            if (captured != null) {
                allNamedCaptured.add(captured);
                if (breakOnFirstMatch) break;
            }
        }

        // merge all named captured
        Map<String, Object> mergedValues = new HashMap<>();
        for (Map<String, Object> namedCaptured : allNamedCaptured) {
            mergedValues.putAll(namedCaptured);
        }

        // return
        return Output
            .builder()
            .values(mergedValues)
            .build();
    }

    private Map<String, String> patternDefinitions() {
        return Optional.ofNullable(patternDefinitions).orElse(Collections.emptyMap());
    }

    private List<File> patternsDir() {
        if (this.patternsDir == null || this.patternsDir.isEmpty()) return Collections.emptyList();

        return this.patternsDir
            .stream()
            .map(dir -> runContext.workingDir().resolve(Path.of(dir)))
            .map(Path::toFile)
            .collect(Collectors.toList());
    }

    private List<String> patterns() {
        if (pattern != null) return List.of(pattern);

        if (patterns == null || patterns.isEmpty()) {
            throw new IllegalArgumentException(
                "Missing required configuration, either `pattern` or `patterns` properties must not be empty.");
        }
        return patterns;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The values captured from matching the Grok expressions."
        )
        private final Map<String, Object> values;

    }
}
