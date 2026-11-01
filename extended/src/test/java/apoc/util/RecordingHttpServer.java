package apoc.util;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Local HTTP listener that answers every request with the same JSON body and records what it received,
 * e.g. to assert that a credential has (not) been sent to it.
 */
public class RecordingHttpServer implements AutoCloseable {

    public record RecordedRequest(String method, String path, Headers headers) {
        public String header(String name) {
            return headers.getFirst(name);
        }
    }

    private final HttpServer server;
    private final List<RecordedRequest> requests = new CopyOnWriteArrayList<>();

    public RecordingHttpServer(String responseBody) throws IOException {
        byte[] body = responseBody.getBytes(StandardCharsets.UTF_8);
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/", exchange -> {
            exchange.getRequestBody().readAllBytes();
            requests.add(new RecordedRequest(exchange.getRequestMethod(), exchange.getRequestURI().getPath(), exchange.getRequestHeaders()));
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.start();
    }

    public int getPort() {
        return server.getAddress().getPort();
    }

    public List<RecordedRequest> getRequests() {
        return List.copyOf(requests);
    }

    public void clear() {
        requests.clear();
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
