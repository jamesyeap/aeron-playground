package publisher;

import com.aeronplayground.sbe.CounterValueDecoder;
import com.aeronplayground.sbe.CounterValueEncoder;
import com.aeronplayground.sbe.MessageHeaderDecoder;
import com.aeronplayground.sbe.MessageHeaderEncoder;
import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;

public class PublisherAgent implements Agent {
    private static final Logger LOGGER = LoggerFactory.getLogger(PublisherAgent.class);

    CachedEpochClock clock = new CachedEpochClock();
    private long count = 0;
    private Aeron aeron;
    private Publication publication;
    private long recordingId = -1;
    private AeronArchive archiveClient;
    private int intervalInMs = 500;

    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final CounterValueEncoder counterValueEncoder = new CounterValueEncoder();
    private final CounterValueDecoder counterValueDecoder = new CounterValueDecoder();

    private UnsafeBuffer buffer;

    @Override
    public String roleName() {
        return "Publisher";
    }

    @Override
    public void onStart() {
        // get configs
        String aeronDir = System.getProperty("aeronPlayground.dir");
        String aeronChannel = System.getProperty("aeronPlayground.channel");
        int aeronStream = Integer.parseInt(System.getProperty("aeronPlayground.stream"));
        int replayStream = Integer.parseInt(System.getProperty("aeronPlayground.replayStream"));

        // configs to connect to Aeron Archive
        String controlRequestChannel = System.getProperty("aeronPlayground.controlRequestChannel");
        int controlRequestStream = Integer.parseInt(System.getProperty("aeronPlayground.controlRequestStream"));
        String controlResponseChannel = System.getProperty("aeronPlayground.controlResponseChannel");
        int controlResponseStream = Integer.parseInt(System.getProperty("aeronPlayground.controlResponseStream"));

        // config for how often to send a new message
        intervalInMs = Integer.parseInt(System.getProperty("aeronPlayground.intervalInMs", "500"));

        // create the buffer that we will write messages to
        buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(512, BitUtil.CACHE_LINE_LENGTH));

        // create the configuration
        final Aeron.Context ctx = new Aeron.Context().aeronDirectoryName(aeronDir);

        // create the config for the client to Aeron archive
        AeronArchive.Context archiveCtx = new AeronArchive.Context()
                .aeronDirectoryName(aeronDir)
                .controlRequestChannel(controlRequestChannel)
                .controlRequestStreamId(controlRequestStream)
                .controlResponseChannel(controlResponseChannel)
                .controlResponseStreamId(controlResponseStream);

        // connect to the media driver using the configuration
        aeron = Aeron.connect(ctx);
        archiveClient = AeronArchive.connect(archiveCtx);
        // final Publication publication = archiveClient.addRecordedPublication(aeronChannel, aeronStream);

        // get all the past messages that it has published thus far
        LOGGER.info("Fetching all past messages sent...");
        List<RecordingDetails> recordingIDList = getListOfRecordings(archiveClient, aeronChannel, aeronStream);
        for (RecordingDetails recordingDetails : recordingIDList) {
            try {
                count = startReplay(recordingDetails.recordingId(), aeron, archiveClient, aeronChannel, replayStream);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        // request for Aeron Archive to start recording
        recordingId = startOrExtendRecording(recordingIDList, aeronChannel, aeronStream);

        // if there was no previous recording, we can just create a vanilla publication
        if (recordingIDList.isEmpty()) {
            publication = aeron.addPublication(aeronChannel, aeronStream);

        } else {
            // otherwise, we should extend the last recording - to create a publication based on the last recording, we need to do this as we want to extend the last recording, and to do so the publication must match with the recording.
            RecordingDetails detailsOfLastRecording = recordingIDList.getLast();
            final ChannelUri recordedUri = ChannelUri.parse(detailsOfLastRecording.originalChannel());
            final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
                    .media(recordedUri)
                    .initialPosition(detailsOfLastRecording.stopPosition(), detailsOfLastRecording.initialTermId(), detailsOfLastRecording.termBufferLength())
                    .mtu(detailsOfLastRecording.mtuLength())
                    .sessionId(detailsOfLastRecording.sessionId());

            // UDP recordings have an endpoint to carry over; IPC recordings do not.
            if (null != recordedUri.get(ENDPOINT_PARAM_NAME)) {
                builder.endpoint(recordedUri);
            }

            publication = aeron.addExclusivePublication(builder.build(), detailsOfLastRecording.streamId());
        }

        // wait for a subscriber to connect
        while (!publication.isConnected()) {
            LOGGER.info("Waiting for subscriber...");

            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void onClose() {
        // when the publisher shuts down, request the archive client to stop recording
        LOGGER.info("Publisher shutting down - requesting Aeron Archive to stop recording for subscription ID: {}\n", recordingId);
        archiveClient.stopRecording(recordingId);
        publication.close();

        while (!publication.isClosed()) {
            try {
                LOGGER.info("Waiting for publication to be closed - publication: {}\n", publication);
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        aeron.close();
        archiveClient.close();

        LOGGER.info("Publisher shut down");
    }

    @Override
    public int doWork() {
        // send keep-alives to the Aeron Archive client
        archiveClient.pollForErrorResponse();

        long currentTime = SystemEpochClock.INSTANCE.time();
        if (currentTime < clock.time()) {
            return 1;
        }

        clock.update(currentTime);
        clock.advance(TimeUnit.MILLISECONDS.toMillis(intervalInMs));

        // put the message into the buffer
        counterValueEncoder.wrapAndApplyHeader(buffer, 0, messageHeaderEncoder);
        counterValueEncoder.value(count);

        // try to publish the buffer contents
        // this probably puts the contents of the buffer into the shared memory between this application and the media driver
        final long position = publication.offer(buffer);

        // if the position is negative, that means the buffer was not published
        if (position < 0L) {
            printError(position);
        } else {
            // otherwise, that means the buffer was successfully published
            LOGGER.info("Count successfully published: {}\n", count);
            count++;
        }

        return 1;
    }

    private long startReplay(long recordingID, Aeron aeron, AeronArchive archiveClient, String aeronChannel, int replayStream) throws InterruptedException {
        final long sessionId = archiveClient.startReplay(recordingID, AeronArchive.NULL_POSITION, AeronArchive.REPLAY_ALL_AND_FOLLOW, aeronChannel, replayStream);
        String replayChannel = ChannelUri.addSessionId(aeronChannel, (int) sessionId);
        Subscription replaySubscription = aeron.addSubscription(replayChannel, replayStream);
        while (!replaySubscription.isConnected()) {
            LOGGER.info("Waiting for replay subscription...");
            Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        }

        MutableLong latestCount = new MutableLong();
        FragmentHandler fragmentHandler = (directBuffer, offset, length, header) -> {
            messageHeaderDecoder.wrap(directBuffer, offset);
            counterValueDecoder.wrap(directBuffer, offset + messageHeaderDecoder.encodedLength(),
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

            long lastCount = counterValueDecoder.value();
            LOGGER.info("lastCount: {}\n", lastCount);
            if (latestCount.get() < lastCount) {
                latestCount.set(lastCount);
            }
        };

        int fragmentLimit = 10;
        IdleStrategy idleStrategy = new BusySpinIdleStrategy();

        // start replay
        while (true) {
            final int numFragmentsRead = replaySubscription.poll(fragmentHandler, fragmentLimit);
            if (numFragmentsRead == 0) {
                break;
            }

            // idle before polling again
            idleStrategy.idle(numFragmentsRead);
        }

        return latestCount.get();
    }

    private long startOrExtendRecording(List<RecordingDetails> recordingIDList, String aeronChannel, int aeronStream) {
        if (!recordingIDList.isEmpty()) {
            return archiveClient.extendRecording(recordingIDList.getLast().recordingId(), aeronChannel, aeronStream, SourceLocation.REMOTE);
        }

        return archiveClient.startRecording(aeronChannel, aeronStream, SourceLocation.REMOTE);
    }

    private List<RecordingDetails> getListOfRecordings(AeronArchive archiveClient, String aeronChannel, int aeronStream) {
        List<RecordingDetails> recordingDetailsList = new ArrayList<>();

        RecordingDescriptorConsumer consumer = (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
            recordingDetailsList.add(new RecordingDetails(controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity));
        };

        // look through all the recordings that the archiver has for the channel and stream
        archiveClient.listRecordingsForUri(0L, 100, aeronChannel, aeronStream, consumer);

        return recordingDetailsList;
    }

    private static void printError(long errorCode) {
        if (errorCode == Publication.NOT_CONNECTED) {
            LOGGER.info("NOT_CONNECTED");

        } else if (errorCode == Publication.BACK_PRESSURED) {
            LOGGER.info("BACK_PRESSURED");

        } else if (errorCode == Publication.ADMIN_ACTION) {
            LOGGER.info("ADMIN_ACTION");

        } else if (errorCode == Publication.CLOSED) {
            LOGGER.info("CLOSED");

        } else if (errorCode == Publication.MAX_POSITION_EXCEEDED) {
            LOGGER.info("MAX_POSITION_EXCEEDED");
        }
    }
}
