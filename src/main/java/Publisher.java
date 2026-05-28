import io.aeron.Aeron;
import io.aeron.Publication;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.TimeUnit;

/**
 * A simple publisher that connects to a `channel`, and pushes a message to a `stream` once every second.
 */
public class Publisher {
    public static void main(String[] args) throws InterruptedException {
        // specify this with example below:
        //  -DaeronPlayground.dir=/tmp/media-driver-1
        String aeronDir = System.getProperty("aeronPlayground.dir");

        // create the buffer that we will write messages to
        UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(512, BitUtil.CACHE_LINE_LENGTH));

        // create the configuration
        final Aeron.Context ctx = new Aeron.Context().aeronDirectoryName(aeronDir);

        // connect to the media driver using the configuration
        try (final Aeron aeron = Aeron.connect(ctx);
             final Publication publication = aeron.addPublication("aeron:ipc", 51)) {

            // wait for a subscriber to connect
            while (!publication.isConnected()) {
                System.out.println("Waiting for subscriber...");
                Thread.sleep(TimeUnit.SECONDS.toMillis(1));
            }

            while (true) {
                // put the message into the buffer
                String message = "Hello";
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
                    System.out.println("Message successfully published");
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
