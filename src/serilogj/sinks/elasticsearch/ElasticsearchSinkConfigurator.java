package serilogj.sinks.elasticsearch;

import java.time.Duration;

import serilogj.core.ILogEventSink;

public class ElasticsearchSinkConfigurator {
    public static ILogEventSink elasticsearch(String serverUrl, String index, Duration period) {
        return new ElasticsearchSink(serverUrl, index, null, null, period, null, null);
    }
}
