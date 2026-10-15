package software.aws.connect;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import org.junit.Test;

public class TranscriptionWorkerTest {
    private static final String FINAL_RESULT = "{\"type\":\"Results\",\"is_final\":true,\"start\":1.5,\"duration\":2.0,"
            + "\"channel\":{\"alternatives\":[{\"transcript\":\"hello there\",\"confidence\":0.98}]}}";

    @Test
    public void parsesFinalResult() throws IOException {
        Optional<TranscriptSegment> segment = DeepgramStreamingOutputStream.parseFinalResult("customer", FINAL_RESULT);
        assertTrue(segment.isPresent());
        assertEquals("customer", segment.get().speaker());
        assertEquals("hello there", segment.get().text());
        assertEquals(1.5, segment.get().start(), 0.0001);
        assertEquals(3.5, segment.get().end(), 0.0001);
        assertEquals(0.98, segment.get().confidence(), 0.0001);
    }

    @Test
    public void ignoresNonFinalEmptyAndOtherMessages() throws IOException {
        assertFalse(DeepgramStreamingOutputStream.parseFinalResult("customer", FINAL_RESULT.replace("true", "false")).isPresent());
        assertFalse(DeepgramStreamingOutputStream.parseFinalResult("customer", FINAL_RESULT.replace("hello there", "  ")).isPresent());
        assertFalse(DeepgramStreamingOutputStream.parseFinalResult("customer", "{\"type\":\"Metadata\"}").isPresent());
    }

    @Test
    public void formatsTranscriptWithSpeakerLabels() {
        String text = TranscriptionWorker.formatTranscript(Arrays.asList(
                new TranscriptSegment("agent", 0, 1, "How can I help?", 0.9),
                new TranscriptSegment("customer", 2, 3, "I need a lawyer.", 0.9)));
        assertEquals("Agent: How can I help?\nCustomer: I need a lawyer.", text);
    }
}
