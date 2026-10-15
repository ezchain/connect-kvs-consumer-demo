package software.aws.connect;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Minimal Supabase PostgREST client for public.calls and public.call_transcripts. */
public class SupabaseTranscriptStore {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String baseUrl;
    private final String serviceKey;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    public SupabaseTranscriptStore(String baseUrl, String serviceKey) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.serviceKey = serviceKey;
    }

    /** Returns calls.id for the Connect contact id, if the call row exists yet. */
    public Optional<String> findCallId(String contactId) throws IOException, InterruptedException {
        HttpRequest request = request("/rest/v1/calls?select=id&limit=1&connect_contact_id=eq."
                + URLEncoder.encode(contactId, StandardCharsets.UTF_8)).GET().build();
        JsonNode body = send(request);
        if (body.isArray() && body.size() > 0) {
            return Optional.of(body.get(0).path("id").asText());
        }
        return Optional.empty();
    }

    /** Call rows are created after the call ends, so poll until the row appears. */
    public Optional<String> waitForCall(String contactId, long timeoutSeconds, long intervalSeconds)
            throws IOException, InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(timeoutSeconds).toNanos();
        while (true) {
            Optional<String> id = findCallId(contactId);
            if (id.isPresent() || System.nanoTime() >= deadline) {
                return id;
            }
            Thread.sleep(intervalSeconds * 1000);
        }
    }

    /** Insert-or-update the single call_transcripts row for this call (unique on call_id). */
    public void upsertTranscript(String callId, String transcriptText, List<TranscriptSegment> segments,
            String language, String status, String errorMessage) throws IOException, InterruptedException {
        ObjectNode row = MAPPER.createObjectNode();
        row.put("call_id", callId);
        row.put("transcript_text", transcriptText);
        ArrayNode array = row.putArray("segments");
        for (TranscriptSegment s : segments) {
            ObjectNode node = array.addObject();
            node.put("speaker", s.speaker());
            node.put("start", s.start());
            node.put("end", s.end());
            node.put("text", s.text());
            node.put("confidence", s.confidence());
        }
        row.put("language", language);
        row.put("status", status);
        row.put("provider", "deepgram");
        if (errorMessage == null) {
            row.putNull("error_message");
        } else {
            row.put("error_message", errorMessage);
        }
        HttpRequest request = request("/rest/v1/call_transcripts?on_conflict=call_id")
                .header("Content-Type", "application/json")
                .header("Prefer", "resolution=merge-duplicates,return=minimal")
                .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(row)))
                .build();
        send(request);
    }

    public void updateCallTranscriptStatus(String callId, String status) throws IOException, InterruptedException {
        ObjectNode patch = MAPPER.createObjectNode();
        patch.put("transcript_status", status);
        HttpRequest request = request("/rest/v1/calls?id=eq." + URLEncoder.encode(callId, StandardCharsets.UTF_8))
                .header("Content-Type", "application/json")
                .header("Prefer", "return=minimal")
                .method("PATCH", HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(patch)))
                .build();
        send(request);
    }

    private HttpRequest.Builder request(String path) {
        return HttpRequest.newBuilder(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(30))
                .header("apikey", serviceKey)
                .header("Authorization", "Bearer " + serviceKey);
    }

    private JsonNode send(HttpRequest request) throws IOException, InterruptedException {
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        int code = response.statusCode();
        String body = response.body() == null ? "" : response.body();
        if (code < 200 || code >= 300) {
            String shortBody = body.length() > 300 ? body.substring(0, 300) + "..." : body;
            throw new IOException("Supabase " + request.method() + " " + request.uri().getPath()
                    + " failed: HTTP " + code + " " + shortBody);
        }
        return body.isBlank() ? MAPPER.createObjectNode() : MAPPER.readTree(body);
    }
}
