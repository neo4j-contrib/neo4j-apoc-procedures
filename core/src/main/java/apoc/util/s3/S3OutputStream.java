package apoc.util.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import javax.annotation.Nonnull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class S3OutputStream extends OutputStream {
    private volatile boolean isDone = false;
    private volatile long totalMemory = 0L;
    private long transferred = 0;
    private int buffSize = 0;
    private final Object totalMemoryLock = new Object();
    private final Future<?> managerFuture;
    private byte[] buffer;
    private final String bucketName;
    private final String keyName;
    private final BlockingQueue<S3UploadData> queue = new LinkedBlockingQueue<>();
    private int maxWaitTimeMinutes = S3UploadConstants.MAX_WAIT_TIME_MINUTES;

    // Extra constructor to allow user to overwrite maxWaitTimeMinutes.
    S3OutputStream(@Nonnull AmazonS3 s3Client, @Nonnull String bucketName, @Nonnull String keyName, int maxWaitTimeMinutes) throws IOException {
        this(s3Client, bucketName, keyName);
        this.maxWaitTimeMinutes = maxWaitTimeMinutes;
    }

    S3OutputStream(@Nonnull AmazonS3 s3Client, @Nonnull String bucketName, @Nonnull String keyName) throws IOException {
        if (bucketName.isEmpty() || keyName.isEmpty()) {
            throw new InvalidParameterException("Bucket and/or key pass to S3OutputStream is empty.");
        }
        this.bucketName = bucketName;
        this.keyName = keyName;
        allocateMemory(AllocationSize.MB_5);
        ExecutorService executorService = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("S3-Upload-Manager-Thread-%d").setDaemon(true).build());
        managerFuture = executorService.submit(new S3UploadManager(s3Client, queue));
    }

    private void allocateMemory(final AllocationSize allocationSize) throws IOException {
        long incomingSize = allocationSize.getAllocationSize();
        synchronized (totalMemoryLock) {
            if (incomingSize > S3UploadConstants.TOTAL_MEMORY_ALLOWED) {
                throw new IOException(String.format("A total of %d bytes of memory were provided for all buffers, but a buffer of %d bytes was requested.",
                        S3UploadConstants.TOTAL_MEMORY_ALLOWED, incomingSize));
            }
            // Only allow defined amount of memory to be used at one time.
            while ((totalMemory + incomingSize) > S3UploadConstants.TOTAL_MEMORY_ALLOWED) {
                try {
                    // Wait for signal that the amount of memory in use has gone down before allocating more.
                    totalMemoryLock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        // We have enough free memory to continue.
        synchronized (totalMemoryLock) {
            totalMemory += incomingSize;
        }
        buffer = new byte[(int)incomingSize];
    }

    private void transmitBuffer() throws IOException {
        queue.add(new S3UploadData(new ByteArrayInputStream(buffer), false, buffSize));
        transferred += buffSize;
        buffSize = 0;

        /*
            Memory allocation here scales with the amount of memory transferred. From the documentation, S3 multipart
            upload has a 5 MB minimum part size (with the exception of the last part), with a maximum of 10,000 parts
            and a maximum file size of 5 TB. To allow up to 5 TB to be transferred and allow multipart streaming of
            smaller files, the amount of memory allocated scales with the amount of data transferred.
            See https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
         */
        if (transferred < S3UploadConstants.TRANSFERRED_2p5GB) {
            allocateMemory(AllocationSize.MB_5);
        } else if (transferred < S3UploadConstants.TRANSFERRED_25GB) {
            allocateMemory(AllocationSize.MB_50);
        } else if (transferred < S3UploadConstants.TRANSFERRED_2TB) {
            allocateMemory(AllocationSize.MB_500);
        } else {
            allocateMemory(AllocationSize.MB_750);
        }
    }

    @Override
    public void write(final int i) throws IOException {
        write(new byte[] { (byte)i }, 0, 1);
    }

    @Override
    public void write(@Nonnull final byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    // This function call is used directly by OutputStream writer, so it's best that everything routes to it.
    @Override
    public void write(@Nonnull final byte[] b, final int offset, final int length) throws IOException {
        int rdPtr = offset;
        do {
            // If the amount of data left to consume from the input is less than the amount of space in the
            // buffer, fill the remaining space of the buffer with the input, otherwise fully consume the
            // remaining input.
            final int wrAmount = Math.min(buffer.length - buffSize, length - (rdPtr - offset));
            System.arraycopy(b, rdPtr, buffer, buffSize, wrAmount);
            buffSize += wrAmount;
            rdPtr += wrAmount;

            // If the buffer is full, transmit it
            if (buffer.length == buffSize) {
                transmitBuffer();
            }
        } while ((rdPtr - offset) < length);
    }

    @Override
    public void close() {
        // Do not reorder operations, setting done to true first defeats a race condition with the queue being blocked.
        isDone = true;

        // Based on the requirements of multipart upload, the last piece can disobey the sizing requirements
        // See https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
        queue.add(new S3UploadData(new ByteArrayInputStream(buffer), true, buffSize));
        buffer = null;
        try {
            // Wait for manager's future to complete before exiting.
            synchronized (managerFuture) {
                managerFuture.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static class S3UploadData {
        private final InputStream stream;
        private final boolean isLast;
        private final int size;

        S3UploadData(@Nonnull InputStream stream, final boolean isLast, final int size) {
            this.stream = stream;
            this.isLast = isLast;
            this.size = size;
        }

        InputStream getStream() {
            return stream;
        }

        boolean getIsLast() {
            return isLast;
        }

        int getSize() {
            return size;
        }
    }

    public class S3UploadManager implements Runnable {
        private final AmazonS3 s3Client;
        private final String uploadId;
        private final BlockingQueue<S3UploadData> queue;
        private final List<PartETag> partETags = new ArrayList<>();
        private final InitiateMultipartUploadResult initResponse;
        private final ExecutorService executorService = Executors.newFixedThreadPool(
                S3UploadConstants.MAX_THREAD_COUNT,
                new ThreadFactoryBuilder().setNameFormat("S3-Upload-Thread-%d").setDaemon(true).build());

        S3UploadManager(@Nonnull final AmazonS3 s3Client, @Nonnull final BlockingQueue<S3UploadData> queue) {
            this.s3Client = s3Client;
            this.queue = queue;
            initResponse = s3Client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, keyName));
            uploadId = initResponse.getUploadId();
        }

        @Override
        public void run() {
            int partNumber = 1;

            // If the OutputStream is done and the queue is empty, exit the loop.
            while (!isDone || !queue.isEmpty()) {
                try {
                    // Grab item from blocking queue and pass it to executor service.
                    final S3UploadData data = queue.take();
                    executorService.submit(new Uploader(partNumber++, data));
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                }
            }

            // Uploading is complete, finish off remaining parts.
            executorService.shutdown();
            try {
                executorService.awaitTermination(maxWaitTimeMinutes, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            s3Client.completeMultipartUpload(
                    new CompleteMultipartUploadRequest(bucketName, keyName, initResponse.getUploadId(), partETags));
        }

        public class Uploader implements Runnable {
            private final int partNumber;
            private final S3UploadData s3UploadData;

            Uploader(final int partNumber, @Nonnull final S3UploadData s3UploadData) {
                this.partNumber = partNumber;
                this.s3UploadData = s3UploadData;
            }

            @Override
            public void run() {
                // Upload the part and add the part to the tags.
                final UploadPartRequest uploadPartRequest = new UploadPartRequest()
                        .withBucketName(bucketName)
                        .withKey(keyName)
                        .withUploadId(uploadId)
                        .withPartNumber(partNumber)
                        .withInputStream(s3UploadData.getStream())
                        .withPartSize(s3UploadData.getSize())
                        .withLastPart(s3UploadData.getIsLast());
                final UploadPartResult result = s3Client.uploadPart(uploadPartRequest);
                partETags.add(result.getPartETag());

                // Notify main thread that memory that was given is no longer in use.
                synchronized (totalMemoryLock) {
                    totalMemory -= s3UploadData.getSize();
                    totalMemoryLock.notifyAll();
                }
            }
        }
    }

    public enum AllocationSize {
        MB_5 (5 * S3UploadConstants.MB),
        MB_50 (50 * S3UploadConstants.MB),
        MB_500 (500 * S3UploadConstants.MB),
        MB_750 (750 * S3UploadConstants.MB);

        private final int allocationSize;

        AllocationSize(final int allocationSize) {
            this.allocationSize = allocationSize;
        }

        int getAllocationSize() {
            return allocationSize;
        }
    }

    private static class S3UploadConstants {
        private final static int MB = 1024 * 1024;
        private static final long TRANSFERRED_2p5GB = AllocationSize.MB_5.getAllocationSize() * 500L;
        private static final long TRANSFERRED_25GB = AllocationSize.MB_50.getAllocationSize() * 500L;
        private static final long TRANSFERRED_2TB = AllocationSize.MB_500.getAllocationSize() * 4000L;
        private static final long TOTAL_MEMORY_ALLOWED = AllocationSize.MB_750.getAllocationSize() * 3L; // 2.25 GB
        private static final int MAX_THREAD_COUNT = 8;
        // A max of 5 TB could take a very long time, so give a lot of time for this.
        private static final int MAX_WAIT_TIME_MINUTES = 65536;
    }
}
