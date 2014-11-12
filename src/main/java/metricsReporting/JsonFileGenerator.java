package metricsReporting;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class JsonFileGenerator {

    public static void main(String[] args) {

        ObjectMapper mapper = new ObjectMapper();

        Map<String, Object> metric = new HashMap<>();
        metric.put("version", "3.0.0");
        Map<String, Object> gauges = new HashMap<>();
        Map<String, Object> m1 = new HashMap<>();
        m1.put("value", "23923824");
        gauges.put("com.clescot.rest.FooResource.cputime", m1);
        metric.put("gauges", gauges);
        Map<String, Object> counters = new HashMap<>();
        metric.put("counters", counters);
        Map<String, Object> histograms = new HashMap<>();
        metric.put("histograms", histograms);
        Map<String, Object> meters = new HashMap<>();
        Map<String, Object> m2 = new HashMap<>();
        m2.put("count", 17);
        m2.put("m15_rate", 7.644264497709307e-27);
        m2.put("m1_rate", 0.00037290750935655375);
        m2.put("m5_rate", 4.360287931021742e-7);
        m2.put("mean_rate", 0.004765897685820465);
        m2.put("units", "events/second");
        meters.put("com.clescot.rest.FooResource.testException.exceptions", m2);
        metric.put("meters", meters);
        Map<String, Object> timers = new HashMap<>();
        metric.put("timers", timers);

        try {
            mapper.writeValue(new File("/home/ahars/sparky/src/data/metrics.json"), metric);
            String js = mapper.writeValueAsString(metric);
            System.out.println("js = " + js);

            Map<String, Object> map = mapper.readValue(js, Map.class);
            System.out.println("map = " + map.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
