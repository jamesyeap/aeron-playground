import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.TimeUnit;

/**
 * A simple publisher that connects to a `channel`, and pushes a message to a `stream` once every second.
 */
public class Publisher {
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

            int count = 0;
            while (true) {
                // put the message into the buffer
                String message = String.format("Count: %d", count);
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
}
