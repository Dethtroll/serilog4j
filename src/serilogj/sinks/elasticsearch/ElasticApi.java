package serilogj.sinks.elasticsearch;

import serilogj.debugging.SelfLog;

public class ElasticApi {
    private final static String ErrorMarker = "\"errors\":".toLowerCase();

    public static boolean readEventInputResult(String eventInputResult) {
        if (eventInputResult == null) {
            return false;
        }

        int startProp = eventInputResult.toLowerCase().indexOf(ErrorMarker);
        if (startProp == -1) {
            return false;
        }

        int startValue = startProp + ErrorMarker.length();
        if (startValue >= eventInputResult.length()) {
            return false;
        }

        int endValue = eventInputResult.indexOf(',', startValue);
        if (endValue == -1) {
            return false;
        }

        String value = eventInputResult.substring(startValue, endValue);
        try {
            return Boolean.parseBoolean(value);
        } catch (Exception ex) {
            SelfLog.writeLine("Seq returned a minimum level of %s which could not be mapped to LogEventLevel", value);
            return false;
        }
    }
}
