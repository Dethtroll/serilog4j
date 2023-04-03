package serilogj.sinks.elasticsearch;

import serilogj.core.ILogEventSink;

public class ElasticsearchSinkConfigurator {
    public static ILogEventSink elasticsearch(String serverUrl, String index) {
        return new ElasticsearchSink(serverUrl, index, null, null, null, null);
    }
}
