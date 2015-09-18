package ratpack.hadoop.spark.topn.model;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.IOException;

/**
 * Limits the number of unique entities queried on the data file.
 * <p>
 * Immutable value object.
 */
@Getter
@ToString
@EqualsAndHashCode
@JsonSerialize(using = Limit.LimitSerializer.class)
public class Limit {
  private final Integer value;

  private Limit(int value) {
    this.value = value;
  }

  /**
   * Factory method for creating limit with lower and upper boundary checking.
   * @param value a value to convert to the {@link Limit}
   * @return the {@link Limit}
   */
  public static Limit of(int value) {
    if (!inRange(value, 1, Integer.MAX_VALUE)) {
      return null;
    } else {
      return new Limit(value);
    }
  }

  private static boolean inRange(int value, int min, int max) {
    return value >= min && value <= max;
  }

  /**
   * Custom serializer for {@link Limit} value object. Serializes value without {@code value} object.
   */
  public static class LimitSerializer extends JsonSerializer<Limit> {
    @Override
    public void serialize(Limit value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
      gen.writeNumber(value.getValue());
    }
  }
}
