import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StartMediaDriver sets up the MediaDriver that will be used by all applications on this box to send and receive messages.
 * <p>
 * We're running the MediaDriver as a stand-alone process - although it can also be embedded into the same process as one of the applications.
 */
public class StartMediaDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(StartMediaDriver.class);

    public static void main(String[] args) {
        //  -DaeronPlayground.dir=/tmp/media-driver-1
        String aeronDir = System.getProperty("aeronPlayground.dir");

        // create the config for the media driver
        final MediaDriver.Context mediaDriverCtx = new MediaDriver.Context()
                .aeronDirectoryName(aeronDir)
                .sharedIdleStrategy(new BusySpinIdleStrategy())
                .threadingMode(ThreadingMode.DEDICATED);

        // start the media driver
        ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final MediaDriver mediaDriver = MediaDriver.launch(mediaDriverCtx);
        LOGGER.info("Media driver started! Aeron directory: {}\n", aeronDir);

        barrier.await();
        LOGGER.info("Shutting down media driver - Aeron directory: {}\n", aeronDir);
        CloseHelper.closeAll(mediaDriver, barrier);
        LogManager.shutdown();
    }
}