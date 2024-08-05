package io.kestra.plugin.transform.jsonata;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface JSONataInterface {

    @PluginProperty(dynamic = true)
    @Schema(title = "The JSONata expression to apply on the JSON object.")
    @NotNull
    String getExpression();

    @PluginProperty(dynamic = true)
    @Schema(title = "The maximum number of recursive calls allowed for the JSONata transformation.")
    @NotNull
    Integer getMaxDepth();
}
