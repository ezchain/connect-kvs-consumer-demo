package software.aws.connect;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.kinesisvideo.parser.utilities.FragmentMetadataVisitor;
import com.amazonaws.kinesisvideo.parser.examples.KinesisVideoCommon;
import com.amazonaws.kinesisvideo.parser.examples.StreamOps;
import com.amazonaws.kinesisvideo.parser.examples.GetMediaWorker;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesisvideo.model.StartSelector;
import com.amazonaws.services.kinesisvideo.model.StartSelectorType;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

public class LMSStreamConsumer extends KinesisVideoCommon {

    private final ExecutorService executorService;
    private final StreamOps streamOps;
    private final OutputStream outputStreamFromCustomer;
    private final OutputStream outputStreamToCustomer;
    private final String fragmentNumber;
    private final String contactId;
    private long maxDurationSeconds = 120;
    private BooleanSupplier stopCondition;

    public void setMaxDurationSeconds(long maxDurationSeconds) {
        this.maxDurationSeconds = maxDurationSeconds;
    }

    /** Polled once per second; when it returns true the stream is stopped. */
    public void setStopCondition(BooleanSupplier stopCondition) {
        this.stopCondition = stopCondition;
    }

    // constructor with contactId; compare tag with contactId passed in
    public LMSStreamConsumer(Regions region,
                      String streamName,
                      String fragmentNumber,
                      String contactId,
                      AWSCredentialsProvider credentialsProvider,
                      OutputStream outputStreamFromCustomer,
                      OutputStream outputStreamToCustomer) throws IOException {
        super(region, credentialsProvider, streamName);
        this.executorService = Executors.newFixedThreadPool(2);
        this.streamOps = new StreamOps(region,  streamName, credentialsProvider);
        this.outputStreamFromCustomer = outputStreamFromCustomer;
        this.outputStreamToCustomer = outputStreamToCustomer;
        this.fragmentNumber = fragmentNumber;
        this.contactId = contactId;
    }

    // Constructor without contactId; compare tag with first contactId read from the tag
    public LMSStreamConsumer(Regions region,
                      String streamName,
                      String fragmentNumber,
                      AWSCredentialsProvider credentialsProvider,
                      OutputStream outputStreamFromCustomer,
                      OutputStream outputStreamToCustomer) throws IOException {
        this(region, streamName, fragmentNumber, null, credentialsProvider, outputStreamFromCustomer, outputStreamToCustomer);
    }

    public void execute() throws InterruptedException, IOException {
        try {
            FragmentMetadataVisitor fragmentMetadataVisitor = FragmentMetadataVisitor.create();
            LMSTagVisitor lmsTagVisitor = (contactId != null) ? LMSTagVisitor.create(contactId) : LMSTagVisitor.create();
            LMSDataVisitor lmsDataVisitor = LMSDataVisitor.create(fragmentMetadataVisitor, lmsTagVisitor, outputStreamFromCustomer, outputStreamToCustomer);
            
            //Start a GetMedia worker to read and process data from the Kinesis Video Stream.
            GetMediaWorker getMediaWorker = GetMediaWorker.create(
                getRegion(),
                getCredentialsProvider(),
                getStreamName(),
                new StartSelector()
                    .withStartSelectorType(StartSelectorType.FRAGMENT_NUMBER)
                    .withAfterFragmentNumber(fragmentNumber),
                streamOps.getAmazonKinesisVideo(),
                LMSCompositeMkvElementVisitor.create(fragmentMetadataVisitor, lmsTagVisitor, lmsDataVisitor)
            );
            
            Future<?> worker = executorService.submit(getMediaWorker);

            // Wait until the stream ends, the stop condition fires, or the maximum duration elapses.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(maxDurationSeconds);
            while (!worker.isDone() && System.nanoTime() < deadline) {
                if (stopCondition != null && stopCondition.getAsBoolean()) {
                    System.out.println("Stop condition met; ending stream");
                    break;
                }
                Thread.sleep(1000);
            }
            if (!worker.isDone()) {
                System.out.println("Shutting down executor service by force");
                executorService.shutdownNow();
            } else {
                executorService.shutdown();
                System.out.println("Executor service is shutdown");
            }
        } finally {
            outputStreamFromCustomer.close();
            outputStreamToCustomer.close();
        }
    }
}