package apoc.util;

import java.io.File;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

public class PerformanceLoggerSingleton {
	static final MetricRegistry metrics = new MetricRegistry();
	private static PerformanceLoggerSingleton instance = null;

	protected PerformanceLoggerSingleton(String csvFileLocation) {
		ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics).convertRatesTo(TimeUnit.SECONDS)
				.convertDurationsTo(TimeUnit.MILLISECONDS).build();
		reporter.start(10, TimeUnit.SECONDS);

		CsvReporter csvReporter = CsvReporter.forRegistry(metrics).formatFor(Locale.US).convertRatesTo(TimeUnit.SECONDS)
				.convertDurationsTo(TimeUnit.MILLISECONDS).build(new File(csvFileLocation));
		csvReporter.start(10, TimeUnit.SECONDS);

	}

	public static PerformanceLoggerSingleton getInstance(String csvFileLocation) {
		if (instance == null) {
			instance = new PerformanceLoggerSingleton(csvFileLocation);
		}
		return instance;
	}

	public void mark(String meterName) {
		Meter requests = metrics.meter(meterName);
		requests.mark();
	}
	
	public Timer getTimer(String meterName) {
		return metrics.timer(meterName);

	}

}