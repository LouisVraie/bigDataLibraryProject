package scylla.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import scylla.type.Author;

import java.nio.ByteBuffer;
import java.util.Set;

public class AuthorCodec implements TypeCodec<Set<Author>> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public AuthorCodec() {
    }

    @Override
    public GenericType<Set<Author>> getJavaType() {
        return GenericType.setOf(Author.class);
    }

    @Override
    public DataType getCqlType() {
        return DataTypes.setOf(SchemaBuilder.udt(Author.TYPE_NAME, true));
    }

    @Override
    public ByteBuffer encode(Set<Author> authors, ProtocolVersion protocolVersion) {
        try {
            String json = objectMapper.writeValueAsString(authors);
            return ByteBuffer.wrap(json.getBytes());
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Error encoding authors to JSON", e);
        }
    }

    @Override
    public Set<Author> decode(ByteBuffer byteBuffer, ProtocolVersion protocolVersion) {
        try {
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            String json = new String(bytes);
            return (Set<Author>) objectMapper.readValue(json, GenericType.setOf(Author.class).getRawType());
        } catch (Exception e) {
            throw new IllegalArgumentException("Error decoding authors from JSON", e);
        }
    }

    @Override
    public String format(Set<Author> authors) {
        try {
            return objectMapper.writeValueAsString(authors);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Error formatting authors to JSON", e);
        }
    }

    @Override
    public Set<Author> parse(String value) {
        try {
            return (Set<Author>) objectMapper.readValue(value, GenericType.setOf(Author.class).getRawType());
        } catch (Exception e) {
            throw new IllegalArgumentException("Error parsing authors from JSON", e);
        }
    }
}
