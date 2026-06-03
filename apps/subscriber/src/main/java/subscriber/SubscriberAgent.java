package subscriber;

import com.aeronplayground.sbe.CounterValueDecoder;
import com.aeronplayground.sbe.MessageHeaderDecoder;
import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SubscriberAgent implements Agent {

    private static final Logger LOGGER = LoggerFactory.getLogger(SubscriberAgent.class);

    String aeronDir;
    String aeronChannel;
    int aeronStream;

    private Aeron aeron;

    private Subscription subscription;
    private AeronArchive archive;
    private IdleStrategy idleStrategy;

    private FragmentHandler fragmentHandler;
    private final CounterValueDecoder counterValueDecoder = new CounterValueDecoder();
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();

    private boolean shouldReplay = false;
    int fragmentLimit = 10; // TODO: not sure what fragment limit is

    // used to momentarily pause processing of messages from subscription, to see how backpressure works
    private volatile boolean shouldProcess = true;

    public SubscriberAgent(IdleStrategy idleStrategy) {
        this.idleStrategy = idleStrategy;
    }

    @Override
    public String roleName() {
        return "subscriber.Subscriber";
    }

    @Override
    public void onStart() {
        // get configs
        aeronDir = System.getProperty("aeronPlayground.dir");
        aeronChannel = System.getProperty("aeronPlayground.channel");
        aeronStream = Integer.parseInt(System.getProperty("aeronPlayground.stream"));
        int replayStream = Integer.parseInt(System.getProperty("aeronPlayground.replayStream"));

        // configs to connect to Aeron Archive
        shouldReplay = Boolean.parseBoolean(System.getProperty("aeronPlayground.shouldReplay", "false"));
        String controlRequestChannel = System.getProperty("aeronPlayground.controlRequestChannel");
        int controlRequestStream = Integer.parseInt(System.getProperty("aeronPlayground.controlRequestStream"));
        String controlResponseChannel = System.getProperty("aeronPlayground.controlResponseChannel");
        int controlResponseStream = Integer.parseInt(System.getProperty("aeronPlayground.controlResponseStream"));

        // connect to the Media Driver
        final Aeron.Context ctx = new Aeron.Context().aeronDirectoryName(aeronDir);
        aeron = Aeron.connect(ctx);

        // connect to the Aeron Archiver
        AeronArchive.Context archiveCtx = new AeronArchive.Context()
                .aeronDirectoryName(aeronDir)
                .controlRequestChannel(controlRequestChannel)
                .controlRequestStreamId(controlRequestStream)
                .controlResponseChannel(controlResponseChannel)
                .controlResponseStreamId(controlResponseStream);
        archive = AeronArchive.connect(archiveCtx);


        // create handler that contains the business logic
        fragmentHandler = (buffer, offset, length, header) -> {
            // Decode from the fragment offset provided by Aeron.
            messageHeaderDecoder.wrap(buffer, offset);

            int bufferOffset = offset + messageHeaderDecoder.encodedLength();
            counterValueDecoder.wrap(buffer, bufferOffset, messageHeaderDecoder.blockLength(), messageHeaderDecoder.version());
            long value = counterValueDecoder.value();

            LOGGER.info("Received value: {}\n", value);
        };

        if (!shouldReplay) {
            subscription = aeron.addSubscription(aeronChannel, aeronStream);
        } else {
            List<RecordingDetails> recordingDetailsList = getRecordingDetails();
            if (recordingDetailsList.isEmpty()) {
                LOGGER.info("No recordings found! Subscribing to stream directly.");
                subscription = aeron.addSubscription(aeronChannel, aeronStream);
            } else {
                LOGGER.info("Found list of recordings: {}\n", recordingDetailsList);

                RecordingDetails recordingDetails = recordingDetailsList.getLast();
                LOGGER.info("Using the last recording: {}\n", recordingDetails);
                final long sessionId = archive.startReplay(recordingDetails.recordingId(), AeronArchive.NULL_POSITION, AeronArchive.REPLAY_ALL_AND_FOLLOW, aeronChannel, replayStream);
                String replayChannel = ChannelUri.addSessionId(aeronChannel, (int) sessionId);
                subscription = aeron.addSubscription(replayChannel, replayStream);
            }
        }

    }

    @Override
    public int doWork() throws Exception {
        if (!shouldProcess) {
            return 0;
        }

        // note: this is optional - we don't have to wait for the subscription to be connected for the SUBSCRIBER - this is only compulsory for the PUBLISHER
        if (!subscription.isConnected()) {
            LOGGER.info("Waiting for publisher...");
            return 0;
        }

        return subscription.poll(fragmentHandler, fragmentLimit);
    }

    @Override
    public void onClose() {
        LOGGER.info("Shutting down subscriber - closing subscription: {}\n", subscription);
        CloseHelper.closeAll(subscription, aeron, archive);
    }

    public Subscription getSubscription() {
        return subscription;
    }

    public boolean isShouldProcess() {
        return shouldProcess;
    }

    public void setShouldProcess(boolean shouldProcess) {
        this.shouldProcess = shouldProcess;
    }

    private List<RecordingDetails> getRecordingDetails() {
        // look through all the recordings that the archiver has for the channel and stream
        List<RecordingDetails> recordingDetailsList = new ArrayList<>();
        RecordingDescriptorConsumer consumer = (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
            recordingDetailsList.add(new RecordingDetails(controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity));
        };

        archive.listRecordingsForUri(0L, 100, aeronChannel, aeronStream, consumer);

        return recordingDetailsList;
    }
}
