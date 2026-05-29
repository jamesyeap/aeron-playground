import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A simple publisher that connects to a `channel`, and pushes a message to a `stream` once every second.
 */
public class Publisher {
    private static final int REPLAY_STREAM_ID = 53;

    public static void main(String[] args) throws InterruptedException {
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
        UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(512, BitUtil.CACHE_LINE_LENGTH));

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
        try (final Aeron aeron = Aeron.connect(ctx);
             final Publication publication = aeron.addPublication(aeronChannel, aeronStream);
             final AeronArchive archiveClient = AeronArchive.connect(archiveCtx)
             // final Publication publication = archiveClient.addRecordedPublication(aeronChannel, aeronStream);
        ) {
            int count = 0;

            // get all the past messages that it has published thus far
            System.out.println("Fetching all past messages sent...");
            List<Long> recordingIDList = getListOfRecordings(archiveClient, aeronChannel, aeronStream);
            for (Long recordingID: recordingIDList) {
                count = startReplay(recordingID, aeron, archiveClient, aeronChannel, aeronStream);
            }

            // wait for a subscriber to connect
            while (!publication.isConnected()) {
                System.out.println("Waiting for subscriber...");
                Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            }

            // request for Aeron Archive to start recording
            long subscriptionId = archiveClient.startRecording(aeronChannel, aeronStream, SourceLocation.REMOTE);
            // when the publisher shuts down, request the archive client to stop recording
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.format("Publisher shutting down - requesting Aeron Archive to stop recording for subscription ID: %d\n", subscriptionId);
                archiveClient.stopRecording(subscriptionId);
                System.out.println("Publisher shut down");
            }));

            while (true) {
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

                // wait for 1 second before publishing the next message
                Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            }
        }
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

    private static List<Long> getListOfRecordings(AeronArchive archiveClient, String aeronChannel, int aeronStream) {
        List<Long> recordingIdList = new ArrayList<>();

        RecordingDescriptorConsumer consumer = (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
            recordingIdList.add(recordingId);
        };

        // look through all the recordings that the archiver has for the channel and stream
        archiveClient.listRecordingsForUri(0L, 100, aeronChannel, aeronStream, consumer);

        return recordingIdList;
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
}
