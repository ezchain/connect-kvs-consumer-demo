package software.aws.connect;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;

import java.io.FileOutputStream;
import java.io.IOException;

// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

public class LMSAudioExtractor {

    public static void main(String args[]) throws InterruptedException, IOException {
        // Uses the default credentials provider chain: env vars (AWS_ACCESS_KEY_ID /
        // AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN), ~/.aws/credentials profile,
        // or an EC2/ECS/Lambda instance/task role. Set AWS_PROFILE or export the
        // env vars before running if you need a specific identity.
        AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();

        String streamName = resolveStreamName();

        LMSStreamConsumer consumer = new LMSStreamConsumer(
            Regions.fromName(System.getenv().getOrDefault("AWS_REGION", "us-east-1")),
            streamName,
            System.getenv("KVS_START_FRAGMENT_NUMBER"),
            credentialsProvider,
            new FileOutputStream("AudioFromCustomer.raw"),
            new FileOutputStream("AudioToCustomer.raw")
        );

        consumer.execute();
    }

    // GetMediaWorker/StreamOps require a plain stream name, not an ARN.
    // Accept either KVS_STREAM_NAME directly, or derive the name from
    // KVS_STREAM_ARN (format: arn:aws:kinesisvideo:<region>:<account>:stream/<name>/<timestamp>).
    static String resolveStreamName() {
        String streamName = System.getenv("KVS_STREAM_NAME");
        if (streamName != null && !streamName.isEmpty()) {
            return streamName;
        }

        String streamArn = System.getenv("KVS_STREAM_ARN");
        if (streamArn != null && !streamArn.isEmpty()) {
            // stream/<name>/<timestamp>
            String[] parts = streamArn.split(":stream/");
            if (parts.length == 2) {
                String nameAndTimestamp = parts[1];
                int lastSlash = nameAndTimestamp.lastIndexOf('/');
                return (lastSlash > 0) ? nameAndTimestamp.substring(0, lastSlash) : nameAndTimestamp;
            }
        }

        return null;
    }
}