package io.kestra.plugin.transforms.grok;

import com.api.jsonata4java.expressions.EvaluateException;
import com.api.jsonata4java.expressions.Expressions;
import com.api.jsonata4java.expressions.ParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Transform .",
    description = ""
)
@Plugin(
    examples = {
        @Example(
            title = "Transform JSON payload using JSONata expression",
            full = true,
            code = """
                id: example
                namespace: example
                tasks:
                - id: transformJson
                  task: io.kestra.plugin.transforms.grok.GrokTransform
                  # can be either a Kestra URI or a STRING
                  from: {{ previousTask.outputs.uri }}
                  expr: |
                     {
                        "order_id": order_id,
                        "customer_name": customer_name,
                        "total_price": $sum(items.(quantity * price_per_unit))
                     }           
                """
        )
    }
)
public class JSONataTransform extends Task implements RunnableTask<Output> {

    private static final ObjectMapper ION_OBJECT_MAPPER = JacksonMapper.ofIon();

    @Schema(
        title = "The JSON/ION object to transform.",
        description = "Must be a valid JSON string or a `kestra://` internal storage URI."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;

    @PluginProperty(dynamic = true)
    @Schema(title = "The JSONata expression to apply on the JSON/ION object provided through the `from` property.")
    @NotNull
    private String expr;

    @PluginProperty(dynamic = true)
    @Schema(title = "The maximum number of recursive calls allowed for the JSONata transformation.")
    @NotNull
    @Builder.Default
    private Integer maxDepth = 1000;

    @PluginProperty(dynamic = true)
    @Schema(title = "The maximum duration allowed for the evaluation to occur. If it takes longer the task will fail.")
    @NotNull
    @Builder.Default
    private Duration timeout = Duration.ofSeconds(10);


    /**
     * {@inheritDoc}
     **/
    @Override
    public Output run(RunContext runContext) throws Exception {

        final Expressions expressions = parseExpression(runContext);
        final String renderedFrom = runContext.render(this.from);

        final Output output;
        if (renderedFrom.startsWith("kestra://")) {
            URI objectURI = new URI(renderedFrom);
            try (InputStream is = runContext.storage().getFile(objectURI);) {
                Flux<JsonNode> flux = FileSerde.readAll(is, new TypeReference<JsonNode>() {
                });
                output = evaluateExpression(runContext, expressions, flux);
            }
        } else {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(renderedFrom);
            output = evaluateExpression(runContext, expressions, Mono.just(jsonNode).flux());
        }
        return output;
    }

    private Output evaluateExpression(RunContext runContext, Expressions expressions, Flux<JsonNode> flux) throws IOException {
        final Path ouputFilePath = runContext.workingDir().createTempFile(".ion");
        try (final BufferedWriter writer = Files.newBufferedWriter(ouputFilePath)) {
            Long processedItemsTotal = flux.map(throwFunction(jsonNode -> {
                    jsonNode = evaluateExpression(expressions, jsonNode);
                    writer.write(ION_OBJECT_MAPPER.writeValueAsString(jsonNode));
                    writer.newLine();
                    return 1L;
                }))
                .reduce(Long::sum)
                .block();

            writer.flush();

            URI uri = runContext.storage().putFile(ouputFilePath.toFile());
            return Output
                .builder()
                .uri(uri)
                .processedItemsTotal(processedItemsTotal)
                .build();
        } finally {
            Files.deleteIfExists(ouputFilePath); // ensure temp file is deleted in case of error
        }
    }

    private JsonNode evaluateExpression(Expressions expressions, JsonNode jsonNode) {
        try {
            return expressions.evaluate(jsonNode, getTimeout().toMillis(), getMaxDepth());
        } catch (EvaluateException e) {
            throw new RuntimeException("Failed to evaluate expression", e);
        }
    }

    private Expressions parseExpression(RunContext runContext) throws IllegalVariableEvaluationException {
        try {
            return Expressions.parse(runContext.render(this.expr));
        } catch (ParseException | IOException e) {
            throw new IllegalArgumentException("Invalid JSONata expression. Error: " + e.getMessage(), e);
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "File URI containing the result of transformation."
        )
        private final URI uri;

        @Schema(
            title = "The total number of items that was processed by the task."
        )
        private final Long processedItemsTotal;
    }
}
