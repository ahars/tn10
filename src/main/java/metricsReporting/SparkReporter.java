package metricsReporting;

import com.codahale.metrics.*;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * A reporter which publishes metric values to a Spark Receiver.
 *
 */
public class SparkReporter extends ScheduledReporter {

    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    public static class Builder {
        private final MetricRegistry registry;
        private Clock clock;
        private String prefix;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.clock = Clock.defaultClock();
            this.prefix = null;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
        }

        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder prefixedWith(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        public SparkReporter build() {
            return new SparkReporter(registry, clock, prefix, rateUnit, durationUnit, filter);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkReporter.class);

    private final Clock clock;
    private final String prefix;
    private final ObjectMapper mapper = new ObjectMapper();

    private SparkReporter(MetricRegistry registry, Clock clock, String prefix,
                          TimeUnit rateUnit, TimeUnit durationUnit, MetricFilter filter) {
        super(registry, "spark-reporter", filter, rateUnit, durationUnit);
        this.clock = clock;
        this.prefix = prefix;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {

        // nothing to do if we don't have any metrics to report
        if (gauges.isEmpty() && counters.isEmpty() && histograms.isEmpty() && meters.isEmpty() && timers.isEmpty()) {
            LOGGER.info("Waiting for metrics, nothing to report...");
            return;
        }
        final Long timestamp = clock.getTime();

        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            if (entry.getValue().getValue() != null) {

            }
//            reportGauge(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
//            reportCounter(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
//            reportHistogram(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
//            reportMetered(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
//            reportTimer(entry.getKey(), entry.getValue());
        }
    }

    private void reportTimer(String name, Timer timer, Date timestamp) {
        final Snapshot snapshot = timer.getSnapshot();
/*
        spark.send(prefix(name, "max"), format(convertDuration(snapshot.getMax())), timestamp);
        spark.send(prefix(name, "mean"), format(convertDuration(snapshot.getMean())), timestamp);
        spark.send(prefix(name, "min"), format(convertDuration(snapshot.getMean())), timestamp);
        spark.send(prefix(name, "stddev"), format(convertDuration(snapshot.getStdDev())), timestamp);
        spark.send(prefix(name, "p50"), format(convertDuration(snapshot.getMedian())), timestamp);
        spark.send(prefix(name, "p75"), format(convertDuration(snapshot.get75thPercentile())), timestamp);
        spark.send(prefix(name, "p95"), format(convertDuration(snapshot.get95thPercentile())), timestamp);
        spark.send(prefix(name, "p98"), format(convertDuration(snapshot.get98thPercentile())), timestamp);
        spark.send(prefix(name, "p99"), format(convertDuration(snapshot.get99thPercentile())), timestamp);
        spark.send(prefix(name, "p999"), format(convertDuration(snapshot.get999thPercentile())), timestamp);
*/
        reportMetered(name, timer, timestamp);
    }

    private void reportMetered(String name, Metered meter, Date timestamp) {
/*        spark.send(prefix(name, "count"), format(meter.getCount()), timestamp);
        spark.send(prefix(name, "m1_rate"), format(convertRate(meter.getOneMinuteRate())), timestamp);
        spark.send(prefix(name, "m5_rate"), format(convertRate(meter.getFiveMinuteRate())), timestamp);
        spark.send(prefix(name, "m15_rate"), format(convertRate(meter.getFifteenMinuteRate())), timestamp);
        spark.send(prefix(name, "mean_rate"), format(convertRate(meter.getMeanRate())), timestamp);
*/    }

    private void reportHistogram(String name, Histogram histogram, Date timestamp) {
        final Snapshot snapshot = histogram.getSnapshot();
/*        spark.send(prefix(name, "count"), format(histogram.getCount()), timestamp);
        spark.send(prefix(name, "max"), format(snapshot.getMax()), timestamp);
        spark.send(prefix(name, "mean"), format(snapshot.getMean()), timestamp);
        spark.send(prefix(name, "min"), format(snapshot.getMin()), timestamp);
        spark.send(prefix(name, "stddev"), format(snapshot.getStdDev()), timestamp);
        spark.send(prefix(name, "p50"), format(snapshot.getMedian()), timestamp);
        spark.send(prefix(name, "p75"), format(snapshot.get75thPercentile()), timestamp);
        spark.send(prefix(name, "p95"), format(snapshot.get95thPercentile()), timestamp);
        spark.send(prefix(name, "p98"), format(snapshot.get98thPercentile()), timestamp);
        spark.send(prefix(name, "p99"), format(snapshot.get99thPercentile()), timestamp);
        spark.send(prefix(name, "p999"), format(snapshot.get999thPercentile()), timestamp);
*/    }

    private void reportCounter(String name, Counter counter, Date timestamp) {
//        spark.send(prefix(name, "count"), format(counter.getCount()), timestamp);
    }
/*
    private void reportGauge(String name, Gauge gauge, Date timestamp) {
        final Double value = format(gauge.getValue());
        if (value != null) {
//            spark.send(prefix(name, "gauge"), value, timestamp);
        }
    }
*/}
