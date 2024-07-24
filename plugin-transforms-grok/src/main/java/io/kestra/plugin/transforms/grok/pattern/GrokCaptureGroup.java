package io.kestra.plugin.transforms.grok.pattern;

import io.kestra.plugin.transforms.grok.data.Type;
import org.joni.Region;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;

public final class GrokCaptureGroup {

    private final Type type;
    private final String name;
    private final int[] backRefs;

    public GrokCaptureGroup(final String name, final int[] backRefs, final Type type) {
        this.type = Objects.requireNonNull(type, "type cannot be null");
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.backRefs = backRefs;
    }

    /**
     * Gets the type defined for the data field to capture.
     */
    public Type type() {
        return type;
    }

    /**
     * Gets the name defined for the data field to capture.
     */
    public String name() {
        return name;
    }

    /**
     * Gets the {@link GrokCaptureExtractor} to be used for capturing that group.
     *
     * @param consumer the {@link Consumer} to call when a data field is captured.
     */
    public GrokCaptureExtractor getExtractor(final Consumer<Object> consumer) {
        return new RawValueExtractor(backRefs, (s -> consumer.accept(type.convert(s))));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "GrokCaptureGroup{" +
            "type=" + type +
            ", name='" + name + '\'' +
            ", backRefs=" + Arrays.toString(backRefs) +
            '}';
    }

    private record RawValueExtractor(int[] backRefs, Consumer<String> consumer) implements GrokCaptureExtractor {

        /**
         * {@inheritDoc}
         */
        @Override
        public void extract(byte[] bytes, Region region) {
            for (int capture : backRefs) {
                int offset = region.getBeg(capture);
                int length = region.getEnd(capture) - offset;
                if (offset >= 0) {
                    String value = new String(bytes, offset, length, StandardCharsets.UTF_8);
                    consumer.accept(value);
                    break; // we only need to capture the first value.
                }
            }
        }
    }
}
