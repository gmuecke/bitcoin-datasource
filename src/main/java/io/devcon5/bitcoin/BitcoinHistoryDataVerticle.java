package io.devcon5.bitcoin;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.slf4j.LoggerFactory.getLogger;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.IntStream;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;

/**
 *
 */
public class BitcoinHistoryDataVerticle extends AbstractVerticle {

    private static final Logger LOG = getLogger(BitcoinHistoryDataVerticle.class);

    //TODO add an LRU chunk-cache
    private TreeMap<Long, Long> index = new TreeMap<>();
    private String filename;
    private long filesize;
    private int chunkSize;
    private int lineLength;

    @Override
    public void start(final Future<Void> startFuture) throws Exception {

        final JsonObject config = Vertx.currentContext().config();

        this.filename = config.getString("bitcoinFile");
        this.chunkSize = config.getInteger("chunkSize");
        this.lineLength = config.getInteger("lineLength");

        this.filesize = Files.size(Paths.get(filename));
        final int indexPoints = (int) (filesize / chunkSize);
        LOG.debug("Bitcoin datafile: {} bytes, {} chunks, {} bytes chunkSize", filesize, indexPoints, chunkSize);

        //create an index for fast-lookups
        vertx.fileSystem()
             .open(filename, new OpenOptions(), openFile -> createPartialIndex(openFile, indexPoints, result -> {
                 this.index = result;
                 LOG.info("Created partial index with {} datapoints", this.index.size());
                 startFuture.complete();
             }));

        vertx.eventBus().consumer("bitcoinPrice", req -> {

            Long ts = (Long) req.body();
            LOG.info("Using index of size {}", this.index.size());
            readDatapoint(ts, req::reply);

        });
    }

    private void createPartialIndex(final AsyncResult<AsyncFile> openFile,
                                    final int indexPoints,
                                    final Handler<TreeMap<Long, Long>> indexHandler) {

        if (openFile.succeeded()) {
            buildIndexTable(openFile.result(), lineLength * 2, indexPoints, chunkSize, indexHandler);
        } else {
            vertx.close();
            throw new RuntimeException("can not read file " + filename, openFile.cause());
        }
    }

    /**
     * builds a lookup index for fast reading the file. The index creates a tree associating timestamp with offset
     * positions in the file. When a timestamp is searched, the indexed offset positions before and after the
     * searched timestamp are used to define which part of the file should be read from disk and processed further
     * in order to find a specific timestamp.
     *
     * @param file
     *         the handle to the async file
     * @param bufferSize
     *         the size of the read puffer for reading a single chunk. It should be twice as large as the expected line
     *         size
     * @param indexPoints
     *         the number of index points to create
     * @param chunksize
     *         the size of a chunk. Defines how many
     *
     * @return the async file
     */

    private void buildIndexTable(AsyncFile file,
                                 final int bufferSize,
                                 final int indexPoints,
                                 final int chunksize,
                                 final Handler<TreeMap<Long, Long>> resultHandler) {

        /*
         * iterate of the entire dataset making jumps the size of a chunk (chunksize).
         * For each chunk the first bytes are read (buffersize).
         * We do this asynchronously so each chunk-head is mapped to a future.
         * The method completes if all futures have been processed.
         */
        CompositeFuture.all(IntStream.range(1, indexPoints)
                                     .mapToLong(i -> i * chunksize)
                                     .mapToObj(offset -> buildIndexFromChunk(file, bufferSize, offset))
                                     .collect(toList())).setHandler(c -> {
            LOG.debug("Completed reading {} index points ", c.result().size());
            resultHandler.handle(c.result()
                                  .list()
                                  .stream()
                                  .map(e -> (IndexPoint) e)
                                  .collect(toMap(ip -> ip.timestamp, ip -> ip.offset, (k1, k2) -> k1, TreeMap::new)));
        });
    }

    /**
     * Creates an index point for the chunk at the offset position. The index point is the first entry of the chunk.
     *
     * @param file
     *         the async file to read the chunk from
     * @param len
     *         the length of the data to be read and parsed for the index point.
     * @param offset
     *         the offset position in the file
     *
     * @return a future containing the index point read from the head of the chunk
     */
    private Future<IndexPoint> buildIndexFromChunk(final AsyncFile file, final int len, final long offset) {

        final Future<IndexPoint> indexPointFuture = Future.future();
        final Buffer readBuffer = Buffer.buffer(len);
        file.read(readBuffer, 0, offset, len, buildIndexPoint(offset, ip -> {
            /*
             *  This check is needed because we chose to make the read-buffer 2x size of a normal line to ensure we
             *  have at least 1 complete line in buffer. Because of this it may occur that two complete dataline
             *  are in the read buffer which will trigger the handler twice.
             */
            if (!indexPointFuture.isComplete()) {
                indexPointFuture.complete(ip);
            }
        }));
        return indexPointFuture;
    }

    /**
     * Creates a handler to read a datapoint at a specific offset. The offset is needed to create the correct address.
     *
     * @param offset
     *         the offset for which the index point should be created. The final offset is calculated with this offset
     *         and a small position adjustment from the actual dataline. The dataline might start with an incomplete
     *         line that is skipped during processing. The number of bytes skipped are added to this offset to calculate
     *         the exact starting position of the chunk.
     * @param indexPointHandler
     *
     * @return a handler for processing the result of a read operation of the chunk
     */
    private Handler<AsyncResult<Buffer>> buildIndexPoint(final long offset,
                                                         final Handler<IndexPoint> indexPointHandler) {

        return result -> {
            if (result.succeeded()) {
                final Buffer buf = result.result();
                readDatapoints(buf, dp -> {
                    IndexPoint ip = new IndexPoint(dp.getTimestamp(), offset + dp.getOffset());
                    LOG.trace("Creating index point {}", ip);
                    indexPointHandler.handle(ip);
                });
            } else {
                throw new RuntimeException("Could not read file ", result.cause());
            }

        };
    }

    /**
     * One line has the format:
     * <pre>
     *     timestamp,price,volume
     * </pre>
     * <ul>
     * <li>timestamp is unix timestamp in seconds</li>
     * <li>price is in USD in double</li>
     * <li>volume is in USD in double</li>
     * </ul>
     * The method accepts a fixed-length buffer containing at least 1 line. The method scanns the buffer for the
     * first line break, indicating the leading (incomplete) line and setting the starting point of the datapoint.
     * Afterwards it scans for the two separators (,) and looks for the end of the line (another linebreak).
     * By reading the strings between start and first separator (timestamp), the two sepatators (price) and the
     * last separator and the end (volume), it can then parse the data into long,double,double representing the
     * full datapoint.
     *
     * @param buffer
     *         the buffer that was read from the file containing a chunk of data
     * @param handler
     *         the handle that is notified for each read datapoint
     */
    private void readDatapoints(final Buffer buffer, Handler<BitcoinDatapoint> handler) {

        final int len = buffer.length();
        int pos = 0;

        while (pos < len) {
            pos = readNextLine(buffer, pos, handler);
        }

    }

    /**
     * Reads the next line from the buffer, starting at the specifies position. When a complete datapoint is found,
     * the handler gets notified.
     *
     * @param buffer
     *         the buffer to read the line from
     * @param initialOffset
     *         the position in the buffer to start reading
     * @param handler
     *         the handler that is notified once a complete datapoint has been parsed
     *
     * @return the position at the end of the datapoint
     */
    private int readNextLine(final Buffer buffer, int initialOffset, final Handler<BitcoinDatapoint> handler) {

        final int len = buffer.length();
        final int[] separators = { -1, -1 };
        int ptr = 0;
        int current;

        /*
            skip to the beginning of a new line and remember the starting offset
            the data in the buffer might be of this structure:
            xxxxx|.....\ntttttt,pppppp,vvvvv\n.....
                 ^      ^      ^      ^      ^
                 1      2      3      4      5

            1: initialOffset, data before (x) is not contained in the buffer. The initial offset is the global position
               in the file, where the content of this buffer starts
            2: offset: line break of last (incomplete) line, followed by timestamp
            3: separator1: ',' followed by the price
            4: separator2: ',' followed by the volume
            5: position at the end of the line (another linebreak) followed by the next line. Might be complete
         */
        final int offset = findLineBegin(buffer, initialOffset);
        int pos = offset;

        /*
            search the separators. There are two separators ','
             - between timestamp and price
             - between price and volume
             as the separator positions will be used relative to the start of the line, the offset between
             the initial offset and the beginning of the line is substracted
         */
        while (++pos < len && (current = buffer.getByte(pos)) != '\n') {
            if (current == ',' && ptr < 2) {
                separators[ptr++] = pos - offset;
            }
        }
        //if both separators were found before the beginning of the new line
        if (ptr == 2 && separators[0] != separators[1]) {
            handler.handle(new BitcoinDatapoint(offset, buffer.getBuffer(offset, pos), separators[0], separators[1]));
        }
        return pos;
    }

    /**
     * Finds the position of the next line
     *
     * @param buffer
     *         the buffer to search the next line beginning
     * @param start
     *         the start offset in the buffer
     *
     * @return the position of the next line
     */
    private int findLineBegin(final Buffer buffer, int start) {

        final int len = buffer.length();
        int pos = start;
        while (pos < len && buffer.getByte(pos++) != '\n') {
            //noop
        }
        return pos;
    }

    private void readDatapoint(final long timestamp, Handler<JsonObject> datapointHandler) {

        //get the offset of the first chunk or 0 if it's before that
        long startOffset = Optional.ofNullable(this.index.lowerEntry(timestamp))
                                   .map(Map.Entry::getValue)
                                   .orElseGet(() -> 0L);
        //get offset of the next chunk of size of file if its in the last chunk
        long endOffset = Optional.ofNullable(this.index.ceilingEntry(timestamp))
                                 .map(Map.Entry::getValue)
                                 .orElseGet(() -> filesize);
        int chunkSize = (int) (endOffset - startOffset);

        vertx.fileSystem().open(filename, new OpenOptions(), openFile -> {
            if (openFile.succeeded()) {
                final AsyncFile file = openFile.result();
                file.read(Buffer.buffer(chunkSize), 0, startOffset, chunkSize, result -> {
                    if (result.succeeded()) {
                        LOG.info("Reading chunk of size {}", chunkSize);
                        final TreeMap<Long, BitcoinDatapoint> chunkData = new TreeMap<>();
                        final Buffer chunk = result.result();
                        readDatapoints(chunk, dp -> chunkData.put(dp.getTimestamp(), dp));
                        LOG.info("Processed chunk with {} entries", chunkData.size());
                        datapointHandler.handle(chunkData.lowerEntry(timestamp).getValue().toJson());
                        file.close();
                    } else {
                        throw new RuntimeException(result.cause());
                    }
                });
            } else {
                throw new RuntimeException("Could not open file", openFile.cause());
            }
        });
    }

    /**
     * Represent a point in the datafile with a timestamp and a byte-offset. So when a timestamp is searched for,
     * the corresponding byte offset can be returned.
     */
    private static class IndexPoint {

        final long timestamp;
        final long offset;

        public IndexPoint(final long timestamp, final long offset) {

            this.timestamp = timestamp;
            this.offset = offset;
        }

        @Override
        public String toString() {

            final StringBuilder sb = new StringBuilder("IndexPoint{");
            sb.append("timestamp=").append(timestamp);
            sb.append(", offset=").append(offset);
            sb.append('}');
            return sb.toString();
        }
    }

}
