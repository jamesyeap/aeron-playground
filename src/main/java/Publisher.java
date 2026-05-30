import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;

/**
 * A simple publisher that connects to a `channel`, and pushes a message to a `stream` once every second.
 */
public class Publisher {
    private static final Logger LOGGER = LoggerFactory.getLogger(Publisher.class);

    private static final int REPLAY_STREAM_ID = 53;

    private static final class PublisherAgent implements Agent {

        CachedEpochClock clock = new CachedEpochClock();
        private int count = 0;
        private Aeron aeron;
        private Publication publication;
        private AeronArchive archiveClient;
        private UnsafeBuffer buffer;

        @Override
        public void onStart() {
            // get configs
            //  -DaeronPlayground.dir=/tmp/media-driver-1
            String aeronDir = System.getProperty("aeronPlayground.dir");
            //  -DaeronPlayground.channel="aeron:ipc"
            String aeronChannel = System.getProperty("aeronPlayground.channel");
            //  -DaeronPlayground.stream="51"
            int aeronStream = Integer.parseInt(System.getProperty("aeronPlayground.stream"));

            // configs to connect to Aeron Archive
            String controlRequestChannel = System.getProperty("aeronPlayground.controlRequestChannel");
            int controlRequestStream = Integer.parseInt(System.getProperty("aeronPlayground.controlRequestStream"));
            String controlResponseChannel = System.getProperty("aeronPlayground.controlResponseChannel");
            int controlResponseStream = Integer.parseInt(System.getProperty("aeronPlayground.controlResponseStream"));

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
            System.out.println("Fetching all past messages sent...");
            List<RecordingDetails> recordingIDList = getListOfRecordings(archiveClient, aeronChannel, aeronStream);
            for (RecordingDetails recordingDetails : recordingIDList) {
                try {
                    count = startReplay(recordingDetails.recordingId(), aeron, archiveClient, aeronChannel, aeronStream);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }

            // request for Aeron Archive to start recording
            final long recordingId = startOrExtendRecording(recordingIDList, aeronChannel, aeronStream);

            // if there was no previous recording, we can just create a vanilla publication
            if (recordingIDList.isEmpty()) {
                publication = aeron.addPublication(aeronChannel, aeronStream);

            } else {
                // otherwise, we should extend the last recording - to create a publication based on the last recording, we need to do this as we want to extend the last recording, and to do so the publication must match with the recording.
                RecordingDetails detailsOfLastRecording = recordingIDList.getLast();
                final ChannelUri recordedUri = ChannelUri.parse(detailsOfLastRecording.originalChannel);
                final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
                        .media(recordedUri)
                        .initialPosition(detailsOfLastRecording.stopPosition, detailsOfLastRecording.initialTermId, detailsOfLastRecording.termBufferLength)
                        .mtu(detailsOfLastRecording.mtuLength)
                        .sessionId(detailsOfLastRecording.sessionId);

                // UDP recordings have an endpoint to carry over; IPC recordings do not.
                if (null != recordedUri.get(ENDPOINT_PARAM_NAME))
                {
                    builder.endpoint(recordedUri);
                }

                publication = aeron.addExclusivePublication(builder.build(), detailsOfLastRecording.streamId);
            }

            // wait for a subscriber to connect
            while (!publication.isConnected()) {
                System.out.println("Waiting for subscriber...");

                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }

            // when the publisher shuts down, request the archive client to stop recording
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.format("Publisher shutting down - requesting Aeron Archive to stop recording for subscription ID: %d\n", recordingId);
                archiveClient.stopRecording(recordingId);
                publication.close();

                while (!publication.isClosed()) {
                    try {
                        System.out.format("Waiting for publication to be closed - publication: %s\n", publication);
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                System.out.println("Publisher shut down");
            }));

        }

        @Override
        public int doWork() throws Exception {
            long currentTime = SystemEpochClock.INSTANCE.time();
            if (currentTime < clock.time()) {
                return 0;
            }

            clock.update(currentTime);
            clock.advance(TimeUnit.SECONDS.toMillis(1));

            // put the message into the buffer
            String message = Integer.toString(count);
            byte[] messageBytes = message.getBytes();
            buffer.putBytes(0, messageBytes);

            // try to publish the buffer contents
            // this probably puts the contents of the buffer into the shared memory between this application and the media driver
            final long position = publication.offer(buffer);

            // if the position is negative, that means the buffer was not published
            if (position < 0L) {
                printError(position);
            } else {
                // otherwise, that means the buffer was successfully published
                System.out.format("Message successfully published: %s\n", message);
                count++;
            }

            return 1;
        }

        private long startOrExtendRecording(List<RecordingDetails> recordingIDList, String aeronChannel, int aeronStream) {
            if (!recordingIDList.isEmpty()) {
                return archiveClient.extendRecording(recordingIDList.getLast().recordingId(), aeronChannel, aeronStream, SourceLocation.REMOTE);
            }

            return archiveClient.startRecording(aeronChannel, aeronStream, SourceLocation.REMOTE);
        }

        @Override
        public String roleName() {
            return "Publisher";
        }
    }

    public static void main(String[] args) throws InterruptedException {
        IdleStrategy idleStrategy = new BackoffIdleStrategy();
        AgentRunner agentRunner = new AgentRunner(
                idleStrategy, Throwable::printStackTrace, null, new PublisherAgent()
        );
        Thread thread = AgentRunner.startOnThread(agentRunner);
        thread.join();
    }

    private static void printError(long errorCode) {
        if (errorCode == Publication.NOT_CONNECTED) {
            System.out.println("NOT_CONNECTED");

        } else if (errorCode == Publication.BACK_PRESSURED) {
            System.out.println("BACK_PRESSURED");

        } else if (errorCode == Publication.ADMIN_ACTION) {
            System.out.println("ADMIN_ACTION");

        } else if (errorCode == Publication.CLOSED) {
            System.out.println("CLOSED");

        } else if (errorCode == Publication.MAX_POSITION_EXCEEDED) {
            System.out.println("MAX_POSITION_EXCEEDED");

        }
    }

    private static List<RecordingDetails> getListOfRecordings(AeronArchive archiveClient, String aeronChannel, int aeronStream) {
        List<RecordingDetails> recordingDetailsList = new ArrayList<>();

        RecordingDescriptorConsumer consumer = (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
            recordingDetailsList.add(new RecordingDetails(controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity));
        };

        // look through all the recordings that the archiver has for the channel and stream
        archiveClient.listRecordingsForUri(0L, 100, aeronChannel, aeronStream, consumer);

        return recordingDetailsList;
    }

    private static int startReplay(long recordingID, Aeron aeron, AeronArchive archiveClient, String aeronChannel, int aeronStream) throws InterruptedException {
        final long sessionId = archiveClient.startReplay(recordingID, AeronArchive.NULL_POSITION, AeronArchive.REPLAY_ALL_AND_FOLLOW, aeronChannel, REPLAY_STREAM_ID);
        String replayChannel = ChannelUri.addSessionId(aeronChannel, (int) sessionId);
        Subscription replaySubscription = aeron.addSubscription(replayChannel, REPLAY_STREAM_ID);
        while (!replaySubscription.isConnected()) {
            System.out.println("Waiting for replay subscription...");
            Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        }

        MutableInteger latestCount = new MutableInteger();
        FragmentHandler fragmentHandler = (directBuffer, offset, length, header) -> {
            // copy the bytes from over to a new buffer -> TODO: do we need to do this?
            byte[] messageBytes = new byte[length];
            directBuffer.getBytes(offset, messageBytes);
            String str = new String(messageBytes);
            String lastCountString = str.substring(0, str.indexOf('\u0000'));
            Integer lastCount = Integer.parseInt(lastCountString, 10);
            // System.out.format("lastCount: %d\n", lastCount);
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

    private record RecordingDetails(
            long controlSessionId,
            long correlationId,
            long recordingId,
            long startTimestamp,
            long stopTimestamp,
            long startPosition,
            long stopPosition,
            int initialTermId,
            int segmentFileLength,
            int termBufferLength,
            int mtuLength,
            int sessionId,
            int streamId,
            String strippedChannel,
            String originalChannel,
            String sourceIdentity) {
    }
}
