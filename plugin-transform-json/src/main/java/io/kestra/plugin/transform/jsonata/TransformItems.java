package io.kestra.plugin.transform.jsonata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.RunnableTask;
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

import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Transform or query a JSON data using JSONata language.",
    description = "[JSONata](https://jsonata.org/) is a sophisticated query and transformation language for JSON data."
)
@Plugin(
    examples = {
        @Example(
            title = "Transform JSON payload using JSONata expression.",
            full = false,
            code = """
                id: jsonata
                namespace: example
                tasks:
                - id: transformJson
                  type: io.kestra.plugin.transform.jsonata.TransformItems
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
public class TransformItems extends Transform implements RunnableTask<Output> {

    private static final ObjectMapper ION_OBJECT_MAPPER = JacksonMapper.ofIon();

    @Schema(
        title = "The file to be transformed.",
        description = "Must be a `kestra://` internal storage URI."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;

    /**
     * {@inheritDoc}
     **/
    @Override
    public Output run(RunContext runContext) throws Exception {

        init(runContext);

        final URI from = new URI(runContext.render(this.from));

        try (InputStream is = runContext.storage().getFile(from)) {
            Flux<JsonNode> flux = FileSerde.readAll(is, new TypeReference<JsonNode>() {
            });
            final Path ouputFilePath = runContext.workingDir().createTempFile(".ion");
            try {

                // transform
                Flux<JsonNode> values = flux.map(this::evaluateExpression);

                Long processedItemsTotal = FileSerde.writeAll(Files.newOutputStream(ouputFilePath), values).block();

                URI uri = runContext.storage().putFile(ouputFilePath.toFile());

                // output
                return Output
                    .builder()
                    .uri(uri)
                    .processedItemsTotal(processedItemsTotal)
                    .build();
            } finally {
                Files.deleteIfExists(ouputFilePath); // ensure temp file is deleted in case of error
            }
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
