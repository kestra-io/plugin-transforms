package io.kestra.plugin.transform.grok.data;

import io.kestra.plugin.transform.grok.data.internal.TypeConverter;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;

import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;

public enum Type {

    SHORT(Short.class),
    INT(Integer.class),
    LONG(Long.class),
    FLOAT(Float.class),
    DOUBLE(Double.class),
    BOOLEAN(Boolean.class),
    STRING(String.class),
    DURATION(Duration.class);

    private final Class<?> objectType;

    /**
     * Creates a new {@link Type} instance.
     *
     * @param objectType the class-type.
     */
    Type(final Class<?> objectType) {
        this.objectType = objectType;
    }

    /**
     * Converts the specified object to this type.
     *
     * @param o the object to be converted.
     * @return the converted object.
     */
    public Object convert(final Object o) {
        return TypeConverter.newForType(objectType).convertValue(o);
    }

    /**
     * Gets the enum for specified string name.
     *
     * @param value        The enum raw value.
     * @param defaultValue The fallback map for unknown string.
     * @return The Enum.
     * @throws IllegalArgumentException if no enum exists for the specified value.
     */
    public static Type getForNameIgnoreCase(final @Nullable String value, final @NotNull Type defaultValue) {
        if (value == null) throw new IllegalArgumentException("Unsupported value 'null'");
        return Arrays.stream(Type.values())
            .filter(e -> e.name().equals(value.toUpperCase(Locale.ROOT)))
            .findFirst()
            .orElse(defaultValue);
    }
}
