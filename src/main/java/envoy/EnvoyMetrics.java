package envoy;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * This file is heavily coupled with the contents of <b>kafka-all.yaml</b>.
 *
 * @author adam.kotwasinski
 */
public class EnvoyMetrics {

    private static final String STATS_ENDPOINT = "http://localhost:9901/stats";

    private static final Pattern UNKNOWN_REQUEST_METRIC_PATTERN = Pattern
            .compile("kafka\\.b([1-3])\\.request\\.unknown: (\\d+)");

    private static final HttpClient BASIC_HTTP_CLIENT = HttpClient.newHttpClient();

    public static Map<Integer, Integer> collectUnknownRequestCounts()
            throws Exception {

        final Map<Integer, Integer> result = new TreeMap<>();
        for (int i = 1; i <= Environment.ENVOY_BROKER_PORTS.size(); ++i) {
            result.put(i, 0); // Envoy config ('cluster' stat prefix) needs to match here.
        }
        final HttpRequest request = HttpRequest.newBuilder().uri(new URI(STATS_ENDPOINT)).GET().build();
        final HttpResponse<Stream<String>> response = BASIC_HTTP_CLIENT.send(request, BodyHandlers.ofLines());
        response.body().forEach(row -> {

            final Matcher matcher = UNKNOWN_REQUEST_METRIC_PATTERN.matcher(row);
            if (matcher.matches()) {
                final int broker = Integer.parseInt(matcher.group(1));
                final int unknowns = Integer.parseInt(matcher.group(2));
                result.put(broker, unknowns);
            }

        });
        return result;
    }

}
