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
import org.agrona.concurrent.*;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * A simple subscriber that connects to a channel, subscribes to a stream, and prints all messages received from the stream.
 */
public class Subscriber {
    private static final Logger LOGGER = LoggerFactory.getLogger(Subscriber.class);

    private static final class SubscriberAgent implements Agent {

        private static final int REPLAY_STREAM_ID = 61;

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

        public SubscriberAgent(IdleStrategy idleStrategy) {
            this.idleStrategy = idleStrategy;
        }

        @Override
        public String roleName() {
            return "Subscriber";
        }

        @Override
        public void onStart() {
            // get configs
            //  -DaeronPlayground.dir=/tmp/media-driver-1
            aeronDir = System.getProperty("aeronPlayground.dir");
            //  -DaeronPlayground.channel="aeron:ipc"
            aeronChannel = System.getProperty("aeronPlayground.channel");
            //  -DaeronPlayground.stream="51"
            aeronStream = Integer.parseInt(System.getProperty("aeronPlayground.stream"));

            // configs to connect to Aeron Archive
            shouldReplay = Boolean.parseBoolean(System.getProperty("aeronPlayground.shouldReplay", "false"));
            String controlRequestChannel = System.getProperty("aeronPlayground.controlRequestChannel");
            int controlRequestStream = Integer.parseInt(System.getProperty("aeronPlayground.controlRequestStream"));
            String controlResponseChannel = System.getProperty("aeronPlayground.controlResponseChannel");
            int controlResponseStream = Integer.parseInt(System.getProperty("aeronPlayground.controlResponseStream"));

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
            subscription = aeron.addSubscription(aeronChannel, aeronStream);
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

            // wait for subscriber to start
            if (!shouldReplay) {
                // note: this is optional - we don't have to wait for the subscription to be connected for the SUBSCRIBER - this is only compulsory for the PUBLISHER
                while (!subscription.isConnected()) {
                    LOGGER.info("Waiting for publisher...");
                    try {
                        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            } else {
                long lastRecordingId = getLastRecordingId(archive, aeronChannel, aeronStream);
                LOGGER.info("Last recording ID: {}\n", lastRecordingId);

                if (lastRecordingId >= 0) {
                    // otherwise, request the archiver to start replaying on the given channel and stream
                    final long sessionId = archive.startReplay(lastRecordingId, AeronArchive.NULL_POSITION, AeronArchive.REPLAY_ALL_AND_FOLLOW, aeronChannel, REPLAY_STREAM_ID);
                    String replayChannel = ChannelUri.addSessionId(aeronChannel, (int) sessionId);
                    Subscription replaySubscription = aeron.addSubscription(replayChannel, REPLAY_STREAM_ID);

                    // note: this is optional - we don't have to wait for the subscription to be connected for the SUBSCRIBER - this is only compulsory for the PUBLISHER
                    while (!replaySubscription.isConnected()) {
                        LOGGER.info("Waiting for replay subscription...");

                        try {
                            Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    replay(replaySubscription, fragmentHandler, fragmentLimit, idleStrategy);
                }
            }
        }

        @Override
        public int doWork() throws Exception {
            return subscription.poll(fragmentHandler, fragmentLimit);
        }

        @Override
        public void onClose() {
            subscription.close();
            LOGGER.info("Shutting down subscriber - closing subscription: {}\n", subscription);
        }
    }

    public static void main(String[] args) {
        IdleStrategy idleStrategy = new BackoffIdleStrategy(100, 10, TimeUnit.SECONDS.toNanos(1), TimeUnit.SECONDS.toNanos(10));
        ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        AgentRunner runner = new AgentRunner(idleStrategy, Throwable::printStackTrace, null, new SubscriberAgent(idleStrategy));

        AgentRunner.startOnThread(runner);
        barrier.await();

        CloseHelper.closeAll(runner, barrier);
        LogManager.shutdown();
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
