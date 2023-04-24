/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.temporal;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import org.neo4j.values.storable.DurationValue;

import java.time.*;
import java.time.format.DateTimeFormatter;

import static apoc.date.Date.*;
import static apoc.util.DateFormatUtil.*;
import static apoc.util.DurationFormatUtil.getDurationFormat;
import static apoc.util.DurationFormatUtil.getOrCreateDurationPattern;

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

        try {
            DateTimeFormatter formatter = getOrCreate(format);

            if (input instanceof LocalDate) {
                return ((LocalDate) input).format(formatter);
            } else if (input instanceof ZonedDateTime) {
                return ((ZonedDateTime) input).format(formatter);
            } else if (input instanceof LocalDateTime) {
                return ((LocalDateTime) input).format(formatter);
            } else if (input instanceof LocalTime) {
                return ((LocalTime) input).format(formatter);
            } else if (input instanceof OffsetTime) {
                return ((OffsetTime) input).format(formatter);
            } else if (input instanceof DurationValue) {
                return formatDuration(input, format);
            }
        } catch (Exception e){
            throw new RuntimeException("Available formats are:\n" +
                    String.join("\n", getTypes()) +
                    "\nSee also: https://www.elastic.co/guide/en/elasticsearch/reference/5.5/mapping-date-format.html#built-in-date-formats " +
                    "and https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html");
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
        DurationValue duration = ((DurationValue) input);
        
        try {
            String pattern = getOrCreateDurationPattern(format);
            return getDurationFormat(duration, pattern);
        } catch (Exception e){
            throw new RuntimeException("Available formats are:\n" +
                String.join("\n", getTypes()) +
                "\nSee also: https://www.elastic.co/guide/en/elasticsearch/reference/5.5/mapping-date-format.html#built-in-date-formats " +
                "and https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html");
        }
    }

    @UserFunction
	@Description("apoc.temporal.toZonedTemporal('2012-12-23 23:59:59','yyyy-MM-dd HH:mm:ss', 'UTC-hour-offset') parse date string using the specified format to specified timezone")
	public ZonedDateTime toZonedTemporal(@Name("time") String time, @Name(value = "format", defaultValue = DEFAULT_FORMAT) String format, final @Name(value = "timezone", defaultValue = "UTC") String timezone) {
		Long value = parseOrThrow(time, getFormat(format, timezone));
		return value == null ? null : Instant.ofEpochMilli(value).atZone(ZoneId.of(timezone));
	}

}
