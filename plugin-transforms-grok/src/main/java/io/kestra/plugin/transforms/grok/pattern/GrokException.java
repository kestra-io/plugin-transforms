package io.kestra.plugin.transforms.grok.pattern;

import io.kestra.core.exceptions.KestraRuntimeException;

public class GrokException extends KestraRuntimeException {

    public GrokException(final String message) {
        super(message);
    }

    public GrokException(final String message, final Throwable cause) {
        super(message, cause);
    }
}