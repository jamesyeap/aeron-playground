import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.BusySpinIdleStrategy;

/**
 * StartMediaDriver sets up the MediaDriver that will be used by all applications on this box to send and receive messages.
 * <p>
 * We're running the MediaDriver as a stand-alone process - although it can also be embedded into the same process as one of the applications.
 */
public class StartMediaDriver {
    public static void main(String[] args) {
        //  -DaeronPlayground.dir=/tmp/media-driver-1
        String aeronDir = System.getProperty("aeronPlayground.dir");

        // create the config for the media driver
        final MediaDriver.Context mediaDriverCtx = new MediaDriver.Context()
                .aeronDirectoryName(aeronDir)
                .sharedIdleStrategy(new BusySpinIdleStrategy())
                .threadingMode(ThreadingMode.SHARED);

        // start the media driver - note: we use the "try-with" pattern here, so that
        // the `close()` method of the MediaDriver can automatically run after this block is exited - see `AutoClosable`
        try (final MediaDriver mediaDriver = MediaDriver.launch(mediaDriverCtx)) {
            System.out.format("Media driver started! Aeron directory: %s\n", aeronDir);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> System.out.format("Shutting down media driver - Aeron directory: %s\n", aeronDir)));

            // to prevent shutdown
            while (true) {
            }
        }
    }
}