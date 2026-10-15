package software.aws.connect;

/**
 * One finalized piece of transcribed speech.
 *
 * @param speaker    "customer" (AUDIO_FROM_CUSTOMER) or "agent" (AUDIO_TO_CUSTOMER)
 * @param start      seconds from the start of the audio stream
 * @param end        seconds from the start of the audio stream
 * @param text       transcribed text
 * @param confidence Deepgram confidence, 0..1
 */
public record TranscriptSegment(String speaker, double start, double end, String text, double confidence) {
}
