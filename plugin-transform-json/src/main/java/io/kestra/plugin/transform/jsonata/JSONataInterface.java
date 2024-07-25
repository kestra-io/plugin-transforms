package io.kestra.plugin.transform.jsonata;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

import java.time.Duration;

public interface JSONataInterface {

    @PluginProperty(dynamic = true)
    @Schema(title = "The JSONata expression to apply on the JSON object.")
    @NotNull
    String getExpr();

    @PluginProperty(dynamic = true)
    @Schema(title = "The maximum number of recursive calls allowed for the JSONata transformation.")
    @NotNull
    Integer getMaxDepth();

    @PluginProperty(dynamic = true)
    @Schema(title = "The maximum duration allowed for the evaluation to occur. If it takes longer the task will fail.")
    @NotNull
    Duration getTimeout();
}
