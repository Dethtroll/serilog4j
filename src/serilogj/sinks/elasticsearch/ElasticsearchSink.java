package serilogj.sinks.elasticsearch;

import serilogj.core.LoggingLevelSwitch;
import serilogj.debugging.SelfLog;
import serilogj.events.LogEvent;
import serilogj.events.LogEventLevel;
import serilogj.formatting.ITextFormatter;
import serilogj.formatting.json.JsonFormatter;
import serilogj.sinks.periodicbatching.PeriodicBatchingSink;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;

public class ElasticsearchSink extends PeriodicBatchingSink {
    public static final int DefaultBatchPostingLimit = 1000;
    public static final Duration DefaultPeriod = Duration.ofSeconds(2);
    private static final Duration RequiredLevelCheckInterval = Duration.ofMinutes(2);

    private final URL baseUrl;
    private final Long eventBodyLimitBytes;
    private LoggingLevelSwitch levelSwitch;
    private LocalDateTime nextRequiredLevelCheck = LocalDateTime.now().plus(RequiredLevelCheckInterval);
    private final Map<String, String> httpHeaders;

    public ElasticsearchSink(String serverUrl, String indexFormat, String login, String password, Duration period, Long eventBodyLimitBytes, LoggingLevelSwitch levelSwitch) {
        this(serverUrl, indexFormat, true, login, password, null, null, period, eventBodyLimitBytes, levelSwitch);
    }
    public ElasticsearchSink(
            String serverUrl,
            String indexName,
            boolean autoRegisterTemplate,
            String login,
            String password,
            Map<String,String> customHttpHeaders,
            Integer batchSizeLimit,
            Duration period,
            Long eventBodyLimitBytes,
            LoggingLevelSwitch levelSwitch
    ) {
        super(batchSizeLimit == null ? DefaultBatchPostingLimit : batchSizeLimit,
              period == null ? DefaultPeriod : period);

        if (serverUrl == null) {
            throw new IllegalArgumentException("serverUrl");
        }

        if (!serverUrl.endsWith("/")) {
            serverUrl += "/";
        }
        this.eventBodyLimitBytes = eventBodyLimitBytes;
        this.levelSwitch = levelSwitch;

        String rawUrl = serverUrl + indexName + "/_bulk/";
        try {
            baseUrl = new URL(rawUrl);
        } catch (MalformedURLException e) {
            SelfLog.writeLine("Invalid server url format: %s", rawUrl);
            throw new IllegalArgumentException("serverUrl");
        }

        Map<String, String> httpHeaders = new HashMap<>();
        httpHeaders.put("Content-Type", "application/json");
        if(customHttpHeaders != null) {
            httpHeaders.putAll(customHttpHeaders);
        }
        if (login != null && password != null) {
            //httpHeaders.put(AuthHeaderName, "");
        }
        this.httpHeaders = Collections.unmodifiableMap(httpHeaders);
    }

    @Override
    protected void emitBatch(Queue<LogEvent> events) {
        nextRequiredLevelCheck = LocalDateTime.now().plus(RequiredLevelCheckInterval);

        StringWriter payload = new StringWriter();

        ITextFormatter formatter = new JsonFormatter(false, "", false, null);
        try {
            for (LogEvent logEvent : events) {
                if (eventBodyLimitBytes != null) {
                    StringWriter buffer = new StringWriter();
                    formatter.format(logEvent, buffer);
                    String buffered = buffer.toString();

                    if (buffered.length() > eventBodyLimitBytes) {
                        SelfLog.writeLine(
                                "Event JSON representation exceeds the byte size limit of %d set for this sink and will be dropped; data: %s",
                                eventBodyLimitBytes, buffered);
                    } else {
                        payload.write("{\"index\": {}}\n");
                        formatter.format(logEvent, payload);
                        payload.write("\n");
                    }
                } else {
                    payload.write("{\"index\": {}}\n");
                    formatter.format(logEvent, payload);
                    payload.write("\n");
                }
            }
        } catch (IOException e) {
            // Never happens
        }

        try {
            HttpURLConnection con = (HttpURLConnection) baseUrl.openConnection();
            con.setRequestMethod("POST");
            httpHeaders.forEach(con::setRequestProperty);
            con.setDoOutput(true);

            OutputStream os = con.getOutputStream();
            os.write(payload.toString().getBytes("UTF8"));
            os.flush();
            os.close();

            InputStream stream;
            int responseCode = con.getResponseCode();
            if (responseCode < 200 || responseCode >= 300) {
                stream = con.getErrorStream();
            } else {
                stream = con.getInputStream();
            }

            String line = "";
            String response = "";
            BufferedReader in = new BufferedReader(new InputStreamReader(stream, "UTF8"));
            while ((line = in.readLine()) != null) {
                response += line;
            }
            in.close();

            if (responseCode < 200 || responseCode >= 300) {
                throw new IOException(response);
            }

            ElasticApi.readEventInputResult(response);
        } catch (IOException e) {
            SelfLog.writeLine("Error sending events to Seq, exception %s", e.getMessage());
        }
    }

    @Override
    protected void onEmptyBatch() {
        if (levelSwitch != null && nextRequiredLevelCheck.isBefore(LocalDateTime.now())) {
            emitBatch(new LinkedList<LogEvent>());
        }
    }
}
