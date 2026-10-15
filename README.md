# FindLaw KVS Transcriber

Transcribes Amazon Connect calls in real time. For one call it reads the live audio from Kinesis Video Streams (KVS), streams the customer and agent tracks to Deepgram, and saves the transcript to Supabase (`public.call_transcripts`).

It runs as a one-off ECS Fargate task, started per call by the dispatcher Lambda.

## How it works
1. Amazon Connect streams call audio to KVS (`AUDIO_FROM_CUSTOMER` and `AUDIO_TO_CUSTOMER` tracks, linear PCM, 16-bit, 8 kHz, mono).
2. `TranscriptionWorker` reads the stream with `GetMedia`, starting at the given fragment number, until the call ends.
3. Each track is sent to its own Deepgram live-transcription WebSocket (`DeepgramStreamingOutputStream`), labelled `customer` and `agent`.
4. When the call's `calls` row exists, the worker upserts one `call_transcripts` row (full text plus per-segment JSON) and updates `calls.transcript_status`.

## Configuration
Set by the dispatcher on the task:

| Variable | Description |
|---|---|
| `CONNECT_CONTACT_ID` | Amazon Connect contact id (matches `calls.connect_contact_id`) |
| `KVS_STREAM_ARN` | ARN of the customer audio stream (`KVS_STREAM_NAME` also works) |
| `KVS_START_FRAGMENT_NUMBER` | Fragment to start reading from |

Optional:

| Variable | Default | Description |
|---|---|---|
| `AWS_REGION` | `us-east-1` | Region of KVS and Secrets Manager |
| `DEEPGRAM_SECRET_NAME` | `findlaw/deepgram/api-key` | Secret with key `DEEPGRAM_API_KEY` |
| `SUPABASE_SECRET_NAME` | `findlaw/supabase-keys` | Secret with keys `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` |
| `DEEPGRAM_MODEL` | `nova-2` | Deepgram model |
| `DEEPGRAM_LANGUAGE` | `en` | `en` or `es` |
| `IDLE_TIMEOUT_SECONDS` | `30` | Stop when no audio arrives for this long |
| `MAX_CALL_SECONDS` | `14400` | Hard limit on stream duration |
| `CALL_LOOKUP_TIMEOUT_SECONDS` | `600` | How long to wait for the `calls` row to appear |

`SUPABASE_SERVICE_ROLE_KEY` must be the `service_role` key, not the `anon` key. The anon key is subject to row-level security and silently returns no rows.

Exit codes: `0` completed, `1` failed, `2` the `calls` row never appeared (transcript not saved).

## AWS permissions
The ECS **task role** needs:
* `secretsmanager:GetSecretValue` on the two secrets above
* `kinesisvideo:GetDataEndpoint`, `kinesisvideo:GetMedia` and `kinesisvideo:DescribeStream` on the call audio streams

## Build
Requires JDK 17 and Maven.

```shell
mvn clean package
```

This runs the tests and produces `target/findlaw-kvs-transcriber-1.0.0-jar-with-dependencies.jar`.

## Docker
```shell
docker build --platform linux/amd64 -t findlaw-transcription-worker:latest .
```

## Code layout
* `TranscriptionWorker` - entry point; wires everything together.
* `LMSStreamConsumer` - reads media from KVS and runs the visitors until the stream ends, a stop condition fires, or the time limit is reached.
* `LMSDataVisitor` - splits frames into the customer and agent output streams by track name.
* `LMSTagVisitor` - reads the MKV tags and stops when the stream moves on to a different contact.
* `LMSCompositeMkvElementVisitor` - combines the visitors above.
* `DeepgramStreamingOutputStream` - sends PCM to Deepgram and collects final segments.
* `SupabaseTranscriptStore` - PostgREST client for `calls` and `call_transcripts`.
* `Secrets` - reads JSON secrets from AWS Secrets Manager.
* `LMSAudioExtractor` - local tool that writes both tracks to `AudioFromCustomer.raw` and `AudioToCustomer.raw` for debugging. Import them in Audacity as raw data: signed 16-bit PCM, 8000 Hz, mono. These files contain call audio, so they are git-ignored.
