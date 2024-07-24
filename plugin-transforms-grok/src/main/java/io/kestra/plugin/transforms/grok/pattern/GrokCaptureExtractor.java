package io.kestra.plugin.transforms.grok.pattern;

import org.joni.Region;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface GrokCaptureExtractor {

    void extract(final byte[] bytes, final Region region);

    class MapGrokCaptureExtractor implements GrokCaptureExtractor {

        private final List<GrokCaptureExtractor> extractors;

        private final Map<String, Object> captured = new HashMap<>();

        public MapGrokCaptureExtractor(final List<GrokCaptureGroup> grokCaptureGroups) {
            this.extractors = grokCaptureGroups
                .stream()
                .map(group -> group.getExtractor(o -> captured.put(group.name(), o)))
                .collect(Collectors.toList());
        }

        @Override
        public void extract(final byte[] bytes, final Region region) {
            for (GrokCaptureExtractor extractor : extractors) {
                extractor.extract(bytes, region);
            }
        }

        public Map<String, Object> captured() {
            return captured;
        }
    }
}
