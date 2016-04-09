package apoc.date;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.time.temporal.TemporalQuery;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.Consumer;
import java.util.stream.Stream;
import apoc.result.LongResult;
import apoc.result.MapResult;
import apoc.result.StringResult;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;


/**
 * @author tkroman
 * @since 9.04.2016
 */
public class Date {
	private static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";
	private static final int MILLIS_IN_SECOND = 1000;
	private static final String UTC_ZONE_ID = "UTC";
	private static final List<TemporalQuery<Consumer<Map<String, Object>>>> DT_FIELDS_SELECTORS = Arrays.asList(
			temporalQuery(ChronoField.YEAR),
			temporalQuery(ChronoField.MONTH_OF_YEAR),
			temporalQuery(ChronoField.DAY_OF_MONTH),
			temporalQuery(ChronoField.HOUR_OF_DAY),
			temporalQuery(ChronoField.MINUTE_OF_HOUR),
			temporalQuery(ChronoField.SECOND_OF_MINUTE),
			temporal -> map -> Optional.ofNullable(TemporalQueries.zone().queryFrom(temporal))
					.ifPresent(zoneId -> map.put("ZoneId", zoneId.getId()))
	);

	@Procedure
	public Stream<MapResult> listFields(final @Name("date") String date) {
		return listFieldsFormatted(date, null);
	}

	@Procedure
	public Stream<MapResult> listFieldsFormatted(final @Name("date") String date, final @Name("pattern") String pattern) {
		if (date == null) {
			return Stream.of(MapResult.empty());
		}
		DateTimeFormatter fmt = getDateTimeFormatter(pattern);
		TemporalAccessor temporal = fmt.parse(date);
		Map<String, Object> selectFields = new HashMap<>();

		for (final TemporalQuery<Consumer<Map<String, Object>>> query : DT_FIELDS_SELECTORS) {
			query.queryFrom(temporal).accept(selectFields);
		}

		return Stream.of(new MapResult(selectFields));
	}

	@Procedure
	public Stream<StringResult> fromSeconds(final @Name("seconds") long unixtime) {
		return fromSecondsFormatted(unixtime, null);
	}

	@Procedure
	public Stream<StringResult> fromSecondsFormatted(final @Name("seconds") long unixtime, final @Name("pattern") String pattern) {
		if (unixtime < 0) {
			throw new IllegalArgumentException("The time argument should be >= 0, got: " + unixtime);
		}
		return Stream.of(new StringResult(getFormat(pattern).format(new java.util.Date(unixtime * MILLIS_IN_SECOND))));
	}

	@Procedure
	public Stream<LongResult> toSeconds(final @Name("date") String dateField) {
		return toSecondsFormatted(dateField, null);
	}

	@Procedure
	public Stream<LongResult> toSecondsFormatted(final @Name("date") String dateField, final @Name("pattern") String pattern) {
		if (dateField == null) {
			return null;
		}
		DateFormat format = getFormat(pattern);
		java.util.Date parse = parseOrThrow(dateField, format);
		return Stream.of(new LongResult(parse.getTime() / 1000));
	}

	private static DateFormat getFormat(final String pattern) {
		String actualPattern = getPattern(pattern);
		SimpleDateFormat format = new SimpleDateFormat(actualPattern);
		if (!(containsTimeZonePattern(actualPattern))) {
			format.setTimeZone(TimeZone.getTimeZone(UTC_ZONE_ID));
		}
		return format;
	}

	private static DateTimeFormatter getDateTimeFormatter(final String pattern) {
		String actualPattern = getPattern(pattern);
		DateTimeFormatter fmt = DateTimeFormatter.ofPattern(actualPattern);
		if (!containsTimeZonePattern(actualPattern)) {
			return fmt.withZone(ZoneId.of(UTC_ZONE_ID));
		} else {
			return fmt;
		}
	}

	private static java.util.Date parseOrThrow(final String date, final DateFormat format) {
		final java.util.Date parsed;
		try {
			parsed = format.parse(date);
		} catch (ParseException e) {
			throw new IllegalArgumentException(e);
		}
		return parsed;
	}

	private static boolean containsTimeZonePattern(final String pattern) {
		return pattern.matches("[XZz]{1,3}");	// doesn't account for strings escaped with "'" (TODO?)
	}

	private static String getPattern(final String pattern) {
		return pattern == null ? DEFAULT_FORMAT : pattern;
	}

	private static TemporalQuery<Consumer<Map<String, Object>>> temporalQuery(final ChronoField field) {
		return temporal -> map -> {
			if (field.isSupportedBy(temporal)) {
				map.put(field.getBaseUnit().toString(), field.getFrom(temporal));
			}
		};
	}
}
