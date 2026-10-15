package software.aws.connect;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * ECS entry point. Reads one Connect call's KVS audio, streams both tracks to Deepgram,
 * and writes the transcript to Supabase (public.call_transcripts).
 *
 * Environment (set by the dispatcher): CONNECT_CONTACT_ID, KVS_STREAM_ARN, KVS_START_FRAGMENT_NUMBER.
 * Optional: AWS_REGION, DEEPGRAM_SECRET_NAME, SUPABASE_SECRET_NAME, DEEPGRAM_MODEL, DEEPGRAM_LANGUAGE (en|es),
 * IDLE_TIMEOUT_SECONDS, MAX_CALL_SECONDS, CALL_LOOKUP_TIMEOUT_SECONDS.
 *
 * Exit codes: 0 completed, 1 failed (failure saved to Supabase when possible), 2 call row never appeared.
 */
public class TranscriptionWorker {
    private static final int SAMPLE_RATE = 8000;
    private static final Set<String> LANGUAGES = Set.of("en", "es");

    public static void main(String[] args) {
        int exitCode = 1;
        try {
            exitCode = run();
        } catch (Throwable t) {
            System.err.println("Transcription worker failed: " + t);
            t.printStackTrace();
        } finally {
            System.out.flush();
            // The GetMedia thread can still be blocked on a socket read; don't wait for it.
            System.exit(exitCode);
        }
    }

    static int run() throws Exception {
        String contactId = requireEnv("CONNECT_CONTACT_ID");
        String streamName = LMSAudioExtractor.resolveStreamName();
        if (streamName == null) {
            throw new IllegalStateException("Missing KVS_STREAM_ARN or KVS_STREAM_NAME");
        }
        String fragment = requireEnv("KVS_START_FRAGMENT_NUMBER");
        String region = env("AWS_REGION", "us-east-1");
        String language = env("DEEPGRAM_LANGUAGE", "en");
        if (!LANGUAGES.contains(language)) {
            throw new IllegalStateException("DEEPGRAM_LANGUAGE must be one of " + LANGUAGES + " (calls.language constraint)");
        }
        String model = env("DEEPGRAM_MODEL", "nova-2");
        Duration idleTimeout = Duration.ofSeconds(Long.parseLong(env("IDLE_TIMEOUT_SECONDS", "30")));
        long maxCallSeconds = Long.parseLong(env("MAX_CALL_SECONDS", "14400"));
        long lookupTimeoutSeconds = Long.parseLong(env("CALL_LOOKUP_TIMEOUT_SECONDS", "600"));

        String deepgramSecretName = env("DEEPGRAM_SECRET_NAME", "findlaw/deepgram/api-key");
        String supabaseSecretName = env("SUPABASE_SECRET_NAME", "findlaw/supabase-keys");
        String deepgramKey = Secrets.require(Secrets.load(region, deepgramSecretName), "DEEPGRAM_API_KEY", deepgramSecretName);
        JsonNode supabaseSecret = Secrets.load(region, supabaseSecretName);
        SupabaseTranscriptStore store = new SupabaseTranscriptStore(
                Secrets.require(supabaseSecret, "SUPABASE_URL", supabaseSecretName),
                Secrets.require(supabaseSecret, "SUPABASE_SERVICE_ROLE_KEY", supabaseSecretName));

        System.out.println("Starting transcription for contact " + contactId + " (model=" + model + ", language=" + language + ")");

        List<DeepgramStreamingOutputStream> streams = new ArrayList<>();
        String failure = null;
        try {
            DeepgramStreamingOutputStream fromCustomer = new DeepgramStreamingOutputStream(deepgramKey, "customer", model, language, SAMPLE_RATE);
            streams.add(fromCustomer);
            DeepgramStreamingOutputStream toCustomer = new DeepgramStreamingOutputStream(deepgramKey, "agent", model, language, SAMPLE_RATE);
            streams.add(toCustomer);

            LMSStreamConsumer consumer = new LMSStreamConsumer(Regions.fromName(region), streamName, fragment, contactId,
                    new DefaultAWSCredentialsProviderChain(), fromCustomer, toCustomer);
            consumer.setMaxDurationSeconds(maxCallSeconds);
            // After the call ends no more frames arrive; treat a quiet stream as "call finished".
            consumer.setStopCondition(() -> fromCustomer.isIdleFor(idleTimeout) && toCustomer.isIdleFor(idleTimeout));
            consumer.execute(); // closes both streams, which flushes Deepgram's final results
        } catch (Exception e) {
            failure = e.getClass().getSimpleName() + ": " + e.getMessage();
            System.err.println("Streaming failed: " + failure);
        } finally {
            for (DeepgramStreamingOutputStream stream : streams) {
                stream.close(); // idempotent
            }
        }

        List<TranscriptSegment> segments = new ArrayList<>();
        for (DeepgramStreamingOutputStream stream : streams) {
            segments.addAll(stream.getSegments());
            if (failure == null && stream.getError() != null) {
                failure = "Deepgram connection error: " + stream.getError().getMessage();
            }
        }
        segments.sort(Comparator.comparingDouble(TranscriptSegment::start));
        String status = failure == null ? "completed" : "failed";
        System.out.println("Transcription " + status + ": " + segments.size() + " segments");

        Optional<String> callId = store.waitForCall(contactId, lookupTimeoutSeconds, 10);
        if (callId.isEmpty()) {
            System.err.println("No calls row for contact " + contactId + " after " + lookupTimeoutSeconds + "s; transcript not saved");
            return 2;
        }
        store.upsertTranscript(callId.get(), formatTranscript(segments), segments, language, status, failure);
        store.updateCallTranscriptStatus(callId.get(), status);
        System.out.println("Saved transcript for call " + callId.get() + " (status=" + status + ")");
        return failure == null ? 0 : 1;
    }

    static String formatTranscript(List<TranscriptSegment> segments) {
        StringBuilder text = new StringBuilder();
        for (TranscriptSegment s : segments) {
            if (text.length() > 0) {
                text.append('\n');
            }
            String label = s.speaker().isEmpty() ? "" : Character.toUpperCase(s.speaker().charAt(0)) + s.speaker().substring(1);
            text.append(label).append(": ").append(s.text());
        }
        return text.toString();
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isEmpty() ? defaultValue : value;
    }

    private static String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException("Missing environment variable " + name);
        }
        return value;
    }
}
