package io.devcon5.util;

import static org.slf4j.LoggerFactory.getLogger;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import io.devcon5.bitcoin.BitcoinDatapoint;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;

/**
 *
 */
public class FileIndexer {

    private static final Logger LOG = getLogger(FileIndexer.class);

    public static void main(String... args) throws IOException {

        JsonObject config = Config.fromFile("config/config.json");

        final String file = config.getString("bitcoinFile");
        final int blocksize = config.getInteger("chunkSize");

        //createIndexNIOFiles(file, blocksize);
        createIndexIO(file, blocksize);
    }

    private static void createIndexIO(final String file, final int blocksize) {
        Path filepath = Paths.get(file);

        int initialIndexBlocks = blocksize * 16;

        TreeMap<Long, Long> index = new TreeMap<>();
        LOG.info("Starting indexing file");
        final long start = System.currentTimeMillis();
        long offset = 0;
        try(BufferedInputStream is = new BufferedInputStream(Files.newInputStream(filepath))){

            byte[] buf = new byte[86];
            int result;
            do {
                result = is.read(buf);
                if (result != -1) {
                    index.put(offset, BitcoinDatapoint.fromBuffer(ByteBuffer.wrap(buf), 10, 20).getTimestamp());
                    offset += buf.length;
                }
                //now skip to next chunk
                offset += is.skip(initialIndexBlocks);
            } while ( result != -1);


        } catch (IOException e) {
            LOG.error("Could not read file", e);
        }
        LOG.info("Read {} indexpoints in {} ms", index.size(), System.currentTimeMillis() - start);
    }

    private static void createIndexNIOFiles(final String file, final int blocksize) throws IOException {

        final Path filepath = Paths.get(file);
        long filesize = Files.size(filepath);
        int blocks = (int) (filesize / blocksize);
        int linesPerBlock = blocks / 43;

        LOG.info("Starting indexing file");
        final long start = System.currentTimeMillis();
        List<String> indexPoints = IntStream.range(0, blocks).parallel()
                                            .mapToObj(blockNo -> {
                                                LOG.debug("Reading block no {}", blockNo);

                                                return lineStream(filepath).skip(linesPerBlock * blockNo)
                                                                    .findFirst()
                                                                    .orElse("EMPTY");
                                            })
                                            .collect(Collectors.toList());

        LOG.info("Read {} indexpoints in {} ms", indexPoints.size(), System.currentTimeMillis() - start);
    }

    private static Stream<String> lineStream(final Path filepath) {

        try {
            return Files.lines(filepath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
