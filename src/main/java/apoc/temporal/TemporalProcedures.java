package apoc.temporal;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import org.neo4j.values.storable.DurationValue;

public class TemporalProcedures
{

    /**
     * Format a temporal value to a String
     *
     * @param input     Any temporal type
     * @param format    A valid DateTime format pattern (ie yyyy-MM-dd'T'HH:mm:ss.SSSS)
     * @return
     */
    @UserFunction( "apoc.temporal.format" )
    @Description( "apoc.temporal.format(input, format) | Format a temporal value" )
    public String format(
            @Name( "temporal" ) Object input,
            @Name( value = "format", defaultValue = "yyyy-MM-dd") String format
    ) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);

        if ( input instanceof LocalDate ) {
            return ((LocalDate) input).format(formatter);
        }
        else if ( input instanceof ZonedDateTime ) {
            return ((ZonedDateTime) input).format(formatter);
        }
        else if ( input instanceof LocalDateTime ) {
            return ((LocalDateTime) input).format(formatter);
        }
        else if ( input instanceof LocalTime ) {
            return ((LocalTime) input).format(formatter);
        }
        else if ( input instanceof OffsetTime ) {
            return ((OffsetTime) input).format(formatter);
        }
        else if ( input instanceof DurationValue ) {
            return formatDuration( input, format);
        }

        return input.toString();
    }

    /**
     * Convert a Duration into a LocalTime and format the value as a String
     *
     * @param input
     * @param format
     * @return
     */
    @UserFunction( "apoc.temporal.formatDuration" )
    @Description( "apoc.temporal.formatDuration(input, format) | Format a Duration" )
    public String formatDuration(
            @Name("input") Object input,
            @Name("format") String format
    ) {
        LocalDateTime midnight = LocalDateTime.of(0, 1, 1, 0, 0, 0, 0);
        LocalDateTime newDuration = midnight.plus( (DurationValue) input );

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);

        return newDuration.format(formatter);
    }


}
