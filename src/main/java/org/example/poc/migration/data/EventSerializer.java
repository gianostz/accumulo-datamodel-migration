package org.example.poc.migration.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * JSON UTF-8 serialization for {@link Event} (data-model §2).
 *
 * Determinism (NFR-3): {@link MapperFeature#SORT_PROPERTIES_ALPHABETICALLY} pins the
 * property order so byte-output is identical across JVMs and runs. Without this, record
 * component declaration order would leak through Jackson's reflection cache and a
 * re-imported wave would not be byte-identical.
 */
public final class EventSerializer {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

    public byte[] serialize(Event event) {
        try {
            return MAPPER.writeValueAsBytes(event);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public String serializeToString(Event event) {
        return new String(serialize(event), StandardCharsets.UTF_8);
    }

    public Event deserialize(byte[] json) {
        try {
            return MAPPER.readValue(json, Event.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Event deserialize(String json) {
        return deserialize(json.getBytes(StandardCharsets.UTF_8));
    }
}
