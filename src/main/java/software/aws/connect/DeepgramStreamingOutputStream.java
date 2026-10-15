package software.aws.connect;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * An OutputStream that forwards raw linear16 PCM to a Deepgram live-transcription WebSocket
 * and collects the final transcript segments. Drop-in replacement for the FileOutputStreams
 * used by the original demo, so LMSDataVisitor does not need to change.
 */
public class DeepgramStreamingOutputStream extends OutputStream {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long CONNECT_TIMEOUT_SECONDS = 20;
    private static final long SEND_TIMEOUT_SECONDS = 15;
    private static final long CLOSE_TIMEOUT_SECONDS = 30;

    private final String speaker;
    private final WebSocket webSocket;
    private final List<TranscriptSegment> segments = Collections.synchronizedList(new ArrayList<>());
    private final CompletableFuture<Void> remoteClosed = new CompletableFuture<>();
    private volatile long lastWriteNanos = System.nanoTime();
    private volatile Throwable error;
    private volatile boolean closed;

    public DeepgramStreamingOutputStream(String apiKey, String speaker, String model, String language, int sampleRate)
            throws IOException {
        this.speaker = speaker;
        URI uri = URI.create("wss://api.deepgram.com/v1/listen"
                + "?model=" + enc(model)
                + "&language=" + enc(language)
                + "&encoding=linear16"
                + "&sample_rate=" + sampleRate
                + "&channels=1"
                + "&punctuate=true"
                + "&smart_format=true"
                + "&interim_results=false");
        try {
            this.webSocket = HttpClient.newHttpClient().newWebSocketBuilder()
                    .header("Authorization", "Token " + apiKey)
                    .connectTimeout(Duration.ofSeconds(CONNECT_TIMEOUT_SECONDS))
                    .buildAsync(uri, new Listener())
                    .get(CONNECT_TIMEOUT_SECONDS + 5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while connecting to Deepgram for " + speaker, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IOException("Could not connect to Deepgram for " + speaker + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void write(int b) throws IOException {
        write(new byte[] { (byte) b }, 0, 1);
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        if (closed) {
            throw new IOException("Deepgram stream for " + speaker + " is closed");
        }
        if (error != null) {
            throw new IOException("Deepgram connection failed for " + speaker, error);
        }
        if (webSocket.isOutputClosed()) {
            throw new IOException("Deepgram connection for " + speaker + " was closed by the server");
        }
        if (len == 0) {
            return;
        }
        // The ByteBuffer must not change until the send completes, so hand over a private copy.
        send(webSocket.sendBinary(ByteBuffer.wrap(Arrays.copyOfRange(b, off, off + len)), true));
        lastWriteNanos = System.nanoTime();
    }

    /** Asks Deepgram to flush pending audio, waits for the final results, then closes the socket. */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            if (error == null && !webSocket.isOutputClosed()) {
                send(webSocket.sendText("{\"type\":\"CloseStream\"}", true));
                remoteClosed.get(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException | ExecutionException | TimeoutException e) {
            System.err.println("Deepgram close for " + speaker + " did not finish cleanly: " + e.getMessage());
        } finally {
            webSocket.abort();
        }
    }

    /** Segments finalized so far (complete once close() has returned). */
    public List<TranscriptSegment> getSegments() {
        synchronized (segments) {
            return new ArrayList<>(segments);
        }
    }

    public boolean isIdleFor(Duration duration) {
        return System.nanoTime() - lastWriteNanos > duration.toNanos();
    }

    public Throwable getError() {
        return error;
    }

    static Optional<TranscriptSegment> parseFinalResult(String speaker, String json) throws IOException {
        JsonNode root = MAPPER.readTree(json);
        if (!"Results".equals(root.path("type").asText()) || !root.path("is_final").asBoolean(false)) {
            return Optional.empty();
        }
        JsonNode alternative = root.path("channel").path("alternatives").path(0);
        String text = alternative.path("transcript").asText("").trim();
        if (text.isEmpty()) {
            return Optional.empty();
        }
        double start = root.path("start").asDouble(0);
        double duration = root.path("duration").asDouble(0);
        return Optional.of(new TranscriptSegment(speaker, start, start + duration, text,
                alternative.path("confidence").asDouble(0)));
    }

    private void send(CompletableFuture<WebSocket> future) throws IOException {
        try {
            future.get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while sending to Deepgram for " + speaker, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IOException("Failed sending to Deepgram for " + speaker + ": " + e.getMessage(), e);
        }
    }

    private void handleMessage(String json) {
        try {
            JsonNode root = MAPPER.readTree(json);
            if ("Error".equalsIgnoreCase(root.path("type").asText())) {
                System.err.println("Deepgram error for " + speaker + ": " + root.path("description").asText(json));
                return;
            }
            parseFinalResult(speaker, json).ifPresent(segments::add);
        } catch (IOException e) {
            System.err.println("Unparseable Deepgram message for " + speaker + ": " + e.getMessage());
        }
    }

    private static String enc(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private final class Listener implements WebSocket.Listener {
        private final StringBuilder buffer = new StringBuilder();

        @Override
        public void onOpen(WebSocket ws) {
            ws.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
            buffer.append(data);
            if (last) {
                String message = buffer.toString();
                buffer.setLength(0);
                handleMessage(message);
            }
            ws.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket ws, int statusCode, String reason) {
            remoteClosed.complete(null);
            return null;
        }

        @Override
        public void onError(WebSocket ws, Throwable t) {
            error = t;
            remoteClosed.complete(null);
        }
    }
}
