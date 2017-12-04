package io.devcon5.index;

import static java.util.stream.Collectors.toMap;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import io.devcon5.bitcoin.BufferedOffsetDatapoint;
import io.devcon5.bitcoin.IndexPoint;
import io.devcon5.util.Config;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;

/**
 * An example for how to create an index for a file.
 */
public class FileIndexExample {

    private static final Logger LOG = getLogger(FileIndexExample.class);

    public static void main(String... args) throws IOException, InterruptedException {

        JsonObject config = Config.fromFile("config/config.json");

        final String file = config.getString("bitcoinFile");
        final int blocksize = config.getInteger("chunkSize");
        final int indexChunkSize = config.getInteger("indexChunkSize");

        Executor pool = Executors.newFixedThreadPool(8);

        System.out.println("NIO");
        CountDownLatch latch1 = new CountDownLatch(8);
        IntStream.range(0, 8)
                 .forEach(i -> pool.execute(() -> createIndexNIOFiles(file, blocksize, indexChunkSize, latch1)));
        latch1.await();

        System.out.println("IO");

        CountDownLatch latch2 = new CountDownLatch(8);
        IntStream.range(0, 8).forEach(i -> pool.execute(() -> createIndexIO(file, blocksize, indexChunkSize, latch2)));
        latch2.await();

    }

    private static void createIndexNIOFiles(final String file,
                                            final int blocksize,
                                            int indexBlockSize,
                                            final CountDownLatch latch) {

        final Path filepath = Paths.get(file);
        long filesize = 0;
        try {
            filesize = Files.size(filepath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        int blocks = (int) (filesize / blocksize);

        ByteBuffer bufPool = ByteBuffer.allocate(blocks * indexBlockSize);
        LOG.info("Starting indexing file (NIO)");
        final long start = System.currentTimeMillis();
        try (SeekableByteChannel sbc = Files.newByteChannel(filepath)) {

            TreeMap<Long, Long> index = IntStream.range(0, blocks).map(i -> i * blocksize).mapToObj(offset -> {
                try {
                    sbc.position(offset);
                    bufPool.limit(bufPool.position() + indexBlockSize);
                    sbc.read(bufPool);
                    bufPool.flip();
                    bufPool.limit(indexBlockSize);
                    ByteBuffer buf = bufPool.slice();
                    bufPool.flip();
                    while (buf.hasRemaining() && buf.get() != '\n') {
                    }
                    return IndexPoint.of(offset, new BufferedOffsetDatapoint(buf.position(), buf));
                } catch (IOException e) {
                    LOG.warn("Could not move cursor to {}", offset);
                    throw new RuntimeException(e);
                }
            }).collect(toMap(IndexPoint::getTimestamp, IndexPoint::getOffset, (k1, k2) -> k1, TreeMap::new));

            LOG.info("Read {} indexpoints in {} ms (NIO)", index.size(), System.currentTimeMillis() - start);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

            latch.countDown();
        }
    }

    private static void createIndexIO(final String file,
                                      final int blocksize,
                                      final int indexChunkSize,
                                      final CountDownLatch latch) {

        Path filepath = Paths.get(file);

        int initialIndexBlocks = blocksize * 16;

        TreeMap<Long, Long> index = new TreeMap<>();
        LOG.info("Starting indexing file (IO)");
        final long start = System.currentTimeMillis();
        long offset = 0;
        try (BufferedInputStream is = new BufferedInputStream(Files.newInputStream(filepath))) {

            byte[] buf = new byte[indexChunkSize];
            int result;
            do {
                while (is.read() != '\n') {
                }
                is.skip(1);
                result = is.read(buf);
                if (result != -1) {
                    index.put(offset,
                              IndexPoint.of(offset, new BufferedOffsetDatapoint((int) offset, ByteBuffer.wrap(buf)))
                                        .getTimestamp());
                    offset += buf.length;
                }
                //now skip to next chunk
                offset += is.skip(initialIndexBlocks);
            } while (result != -1);
            LOG.info("Read {} indexpoints in {} ms (IO)", index.size(), System.currentTimeMillis() - start);

        } catch (IOException e) {
            LOG.error("Could not read file", e);
        } finally {
            latch.countDown();
        }
    }
}
