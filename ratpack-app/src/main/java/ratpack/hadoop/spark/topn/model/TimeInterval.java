package ratpack.hadoop.spark.topn.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.IOException;
import java.time.LocalDate;

/**
 * Defines time interval between two dates: {@code dateFrom} and {@code dateTo}.
 * <p>
 * Immutable value object.
 */
@Getter
@ToString
@EqualsAndHashCode
@JsonSerialize(using = TimeInterval.TimeIntervalSerializer.class)
public class TimeInterval {
  private final LocalDate dateFrom;
  private final LocalDate dateTo;

  private TimeInterval(LocalDate dateFrom, LocalDate dateTo) {
    this.dateFrom = dateFrom;
    this.dateTo = dateTo;
  }

  /**
   * Verifies if time interval contains the date.
   * @param date a date to check if belongs to time interval
   * @return true if date is within time interval
   */
  public boolean contains(LocalDate date) {
    if (date == null) {
      return false;
    }
    if (dateFrom.isAfter(date) || dateTo.isBefore(date)) {
      return false;
    }
    return true;
  }

  /**
   * Creates time interval with all checkings.
   * @param dateFrom a date from which time interval starts
   * @param dateTo a date when time interval ends
   * @return the time interval
   */
  public static TimeInterval of(LocalDate dateFrom, LocalDate dateTo) {
    if (dateFrom == null || dateTo == null || dateFrom.isEqual(dateTo) || dateFrom.isAfter(dateTo)) {
      return null;
    }
    return new TimeInterval(dateFrom, dateTo);
  }

  /**
   * Creates time interval from json object.
   * <p>
   * Intentionally it is {@code JsonCreator} factory method.
   * @param dateFrom a starting local date time represeted in string format
   * @param dateTo an ending local date time represented in string format
   * @return the time interval
   */
  @JsonCreator
  public static TimeInterval of(@JsonProperty("dateFrom") String dateFrom, @JsonProperty("dateTo") String dateTo) {
    if (dateFrom == null || dateTo == null) {
      return null;
    }
    return of(LocalDate.parse(dateFrom), LocalDate.parse(dateTo));
  }

  /**
   * Custom serializer to Json, writing the dates in string format.
   */
  public static class TimeIntervalSerializer extends JsonSerializer<TimeInterval> {
    @Override
    public void serialize(TimeInterval value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
      gen.writeStartObject();
      gen.writeStringField("dateFrom", value.getDateFrom().toString());
      gen.writeStringField("dateTo", value.getDateTo().toString());
      gen.writeEndObject();
    }
  }
}
