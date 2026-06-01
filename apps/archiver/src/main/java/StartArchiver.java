import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple class that records all messages from a given channel and stream.
 */
public class StartArchiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(StartArchiver.class);

    public static void main(String[] args) throws InterruptedException {
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        // get config
        String aeronDir = System.getProperty("aeronPlayground.dir");
        String archiveDir = System.getProperty("aeronPlayground.archiveDir");
        String controlRequestChannel = System.getProperty("aeronPlayground.controlRequestChannel");
        int controlRequestStream = Integer.parseInt(System.getProperty("aeronPlayground.controlRequestStream"));
        String replicationChannel = System.getProperty("aeronPlayground.replicationChannel");

        Archive.Context ctx = new Archive.Context()
                .aeronDirectoryName(aeronDir)
                .archiveDirectoryName(archiveDir)
                .controlChannel(controlRequestChannel)
                .controlStreamId(controlRequestStream)
                .replicationChannel(replicationChannel)
                .threadingMode(ArchiveThreadingMode.DEDICATED);

        final Archive archive = Archive.launch(ctx);
        LOGGER.info("Started archiver. Archive directory: {}\n", archiveDir);

        barrier.await();

        LOGGER.info("Shut down archiver. Archive directory: {}\n", archiveDir);
        CloseHelper.closeAll(archive, barrier);
        LogManager.shutdown();
    }
}
