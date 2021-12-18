/*
 * Name:   JSON Deserializer
 * Author: Bhaskar S
 * Date:   11/25/2021
 * Blog:   https://www.polarsparc.com
 */

package com.polarsparc.kstreams.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.polarsparc.kstreams.KafkaStreamsUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class JsonDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper mapper = KafkaStreamsUtils.getObjectMapper();

    private final Class<T> clazz;

    public JsonDeserializer(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        T data;
        try {
            data = mapper.readValue(bytes, clazz);
        }
        catch (Exception ex) {
            throw new SerializationException(ex.getMessage());
        }

        return data;
    }
}
