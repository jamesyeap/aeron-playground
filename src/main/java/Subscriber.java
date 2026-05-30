import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * A simple subscriber that connects to a channel, subscribes to a stream, and prints all messages received from the stream.
 */
public class Subscriber {
    private static final Logger LOGGER = LoggerFactory.getLogger(Subscriber.class);

    private static final int REPLAY_STREAM_ID = 61;

    public static void main(String[] args) throws InterruptedException {
        // get configs
        //  -DaeronPlayground.dir=/tmp/media-driver-1
        String aeronDir = System.getProperty("aeronPlayground.dir");
        //  -DaeronPlayground.channel="aeron:ipc"
        String aeronChannel = System.getProperty("aeronPlayground.channel");
        //  -DaeronPlayground.stream="51"
        int aeronStream = Integer.parseInt(System.getProperty("aeronPlayground.stream"));

        // configs to connect to Aeron Archive
        boolean shouldReplay = Boolean.parseBoolean(System.getProperty("aeronPlayground.shouldReplay", "false"));
        String controlRequestChannel = System.getProperty("aeronPlayground.controlRequestChannel");
        int controlRequestStream = Integer.parseInt(System.getProperty("aeronPlayground.controlRequestStream"));
        String controlResponseChannel = System.getProperty("aeronPlayground.controlResponseChannel");
        int controlResponseStream = Integer.parseInt(System.getProperty("aeronPlayground.controlResponseStream"));

        // create the configuration
        final Aeron.Context ctx = new Aeron.Context().aeronDirectoryName(aeronDir);
        int fragmentLimit = 10; // TODO: not sure what fragment limit is
        IdleStrategy idleStrategy = new BackoffIdleStrategy(100, 10, TimeUnit.SECONDS.toNanos(1), TimeUnit.SECONDS.toNanos(10));

        // create the config for the client to Aeron archive
        AeronArchive.Context archiveCtx = new AeronArchive.Context()
                .aeronDirectoryName(aeronDir)
                .controlRequestChannel(controlRequestChannel)
                .controlRequestStreamId(controlRequestStream)
                .controlResponseChannel(controlResponseChannel)
                .controlResponseStreamId(controlResponseStream);

        // connect to the media driver using the configuration
        try (final Aeron aeron = Aeron.connect(ctx);
             final Subscription subscription = aeron.addSubscription(aeronChannel, aeronStream);
             final AeronArchive archive = AeronArchive.connect(archiveCtx)
        ) {

            FragmentHandler fragmentHandler = (buffer, offset, length, header) -> {
                // copy the bytes from over to a new buffer -> TODO: do we need to do this?
                byte[] messageBytes = new byte[length];
                buffer.getBytes(offset, messageBytes);
                String message = new String(messageBytes);

                System.out.format("Received message: %s\n", message);
            };

            if (!shouldReplay) {
                // note: this is optional - we don't have to wait for the subscription to be connected for the SUBSCRIBER - this is only compulsory for the PUBLISHER
                while (!subscription.isConnected()) {
                    System.out.println("Waiting for publisher...");
                    Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                }

                subscribe(subscription, fragmentHandler, fragmentLimit, idleStrategy);

            } else {
                long lastRecordingId = getLastRecordingId(archive, aeronChannel, aeronStream);
                System.out.format("Last recording ID: %d\n", lastRecordingId);

                if (lastRecordingId == -1) {
                    // if there were no replay recordings, just subscribe as usual
                    subscribe(subscription, fragmentHandler, fragmentLimit, idleStrategy);

                } else {
                    // otherwise, request the archiver to start replaying on the given channel and stream
                    final long sessionId = archive.startReplay(lastRecordingId, AeronArchive.NULL_POSITION, AeronArchive.REPLAY_ALL_AND_FOLLOW, aeronChannel, REPLAY_STREAM_ID);
                    String replayChannel = ChannelUri.addSessionId(aeronChannel, (int) sessionId);
                    Subscription replaySubscription = aeron.addSubscription(replayChannel, REPLAY_STREAM_ID);

                    // note: this is optional - we don't have to wait for the subscription to be connected for the SUBSCRIBER - this is only compulsory for the PUBLISHER
                    while (!replaySubscription.isConnected()) {
                        System.out.println("Waiting for replay subscription...");
                        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                    }

                    replay(replaySubscription, fragmentHandler, fragmentLimit, idleStrategy);
                    subscribe(subscription, fragmentHandler, fragmentLimit, idleStrategy);
                }
            }
        }
    }

    private static void subscribe(Subscription subscription, FragmentHandler fragmentHandler, int fragmentLimit, IdleStrategy idleStrategy) {
        while (true) {
            final int numFragmentsRead = subscription.poll(fragmentHandler, fragmentLimit);

            // idle before polling again
            idleStrategy.idle(numFragmentsRead);
        }
    }

    private static void replay(Subscription subscription, FragmentHandler fragmentHandler, int fragmentLimit, IdleStrategy idleStrategy) {
        while (true) {
            final int numFragmentsRead = subscription.poll(fragmentHandler, fragmentLimit);
            if (numFragmentsRead == 0) {
                break;
            }

            // idle before polling again
            idleStrategy.idle(numFragmentsRead);
        }
    }

    private static long getLastRecordingId(AeronArchive archiveClient, String aeronChannel, int aeronStream) {
        MutableLong lastRecordingId = new MutableLong();
        RecordingDescriptorConsumer consumer = (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
            lastRecordingId.set(recordingId);
        };

        // list the recordings that the archiver has for the channel and stream
        final int foundCount = archiveClient.listRecordingsForUri(0L, 100, aeronChannel, aeronStream, consumer);
        if (foundCount == 0) {
            return -1;
        }

        return lastRecordingId.get();
    }
}
