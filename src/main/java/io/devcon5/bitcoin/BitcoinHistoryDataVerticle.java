package io.devcon5.bitcoin;

import static java.lang.Long.toHexString;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.slf4j.LoggerFactory.getLogger;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
 * Verticle to read historical bitcoin data from a local file. The file is not read into memory for fast access. Instead
 * the verticle uses a partial index for fast random access the file. Only chunks of the file are loaded as requested,
 * while the required chunk is identified using the partial index.
 */
public class BitcoinHistoryDataVerticle extends AbstractVerticle {

    private static final Logger LOG = getLogger(BitcoinHistoryDataVerticle.class);
    //stats
    private TreeMap<Long, Long> index = new TreeMap<>();
    private Set<Long> refineTracker = new HashSet<>();
    //file stats
    private String filename;
    private long filesize;
    private int indexChunkSize;
    private AsyncFile file;
    //index chunk size
    private int chunkSize;
    private long refinements = 0;

    /**
     * Creates a Bitcoin BitcoinDatapoint from this datapoint
     *
     * @param dp
     *         the bitcoin datapoint to convert to json
     *
     * @return a json object representing the datapoint
     */
    private static JsonObject toJson(final BitcoinDatapoint dp) {

        return new JsonObject().put("ts", dp.getTimestamp()).put("price", dp.getPrice()).put("volume", dp.getVolume());
    }

    @Override
    public void start(final Future<Void> startFuture) throws Exception {

        final JsonObject config = Vertx.currentContext().config();

        this.filename = config.getString("bitcoinFile");
        this.filesize = Files.size(Paths.get(this.filename));
        this.indexChunkSize = config.getInteger("indexChunkSize");
        this.chunkSize = config.getInteger("chunkSize");

        LOG.debug("Bitcoin datafile: {} bytes, {} chunks, {} bytes chunkSize",
                  filesize,
                  (filesize / chunkSize),
                  chunkSize);

        //create an index for fast-lookups
        final long start = System.currentTimeMillis();
        vertx.fileSystem().open(this.filename, new OpenOptions(), openFile -> createPartialIndex(openFile, result -> {
            this.index = result;
            LOG.info("Created partial index with {} datapoints in {} ms", this.index.size(), System.currentTimeMillis()-start);
            startFuture.complete();
        }));

        vertx.eventBus().consumer("bitcoinPrice", req -> readDatapoint((Long) req.body(), req::reply));
        final long interval = config.getInteger("statsInterver", 10000);

        //statistics monitor
        vertx.setPeriodic(interval, t -> {
            LOG.info("Index statistics: { entries:{}, refinements:{}, blocksInRefinement:{}, ref:{}}",
                     this.index.size(),
                     this.refinements,
                     this.refineTracker.size(),
                     this.refineTracker);

        });

    }

    @Override
    public void stop() throws Exception {

        this.file.close();
    }

    /**
     * Initializes the history verticle by creating a partial index.
     *
     * @param openFile
     * @param indexHandler
     */
    private void createPartialIndex(final AsyncResult<AsyncFile> openFile,
                                    final Handler<TreeMap<Long, Long>> indexHandler) {

        //we only create a coarse-grained index covering larger blocks than the configured chunk size
        //on accessing any of the chunks, the index gets more fine grained
        final int indexPoints = (int) (this.filesize / (this.chunkSize * 16));
        if (openFile.succeeded()) {
            this.file = openFile.result();
            buildIndexTable(openFile.result(), 0, indexChunkSize, indexPoints, (this.chunkSize * 16), indexHandler);
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
                                 final long startOffset,
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
                                     .mapToLong(i -> startOffset + i * chunksize)
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
        this.refineTracker.add(offset);

        final long start = System.currentTimeMillis();
        file.read(readBuffer, 0, offset, len, buildIndexPoint(offset, ip -> {
            /*
             *  This check is needed because we chose to make the read-buffer 2x size of a normal line to ensure we
             *  have at least 1 complete line in buffer. Because of this it may occur that two complete dataline
             *  are in the read buffer which will trigger the handler twice.
             */
            if (!indexPointFuture.isComplete()) {
                LOG.debug("Read index point for {} in {} ms", ip.timestamp, System.currentTimeMillis() - start);
                this.refineTracker.remove(offset);
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
                    IndexPoint ip = IndexPoint.of(offset, dp);
                    LOG.trace("Creating index point {}", ip);
                    indexPointHandler.handle(ip);
                });
            } else {
                throw new RuntimeException("Could not read file ", result.cause());
            }

        };
    }

    /**
     * Reads a datapoint for the given timestamp.
     * If the index block is too large, the index gets refined first.
     *
     * @param timestamp
     * @param datapointHandler
     */
    private void readDatapoint(final long timestamp, final Handler<JsonObject> datapointHandler) {
        //get the offset of the first chunk or 0 if it's before that
        long startOffset = getStartOffset(timestamp);
        //get offset of the next chunk of size of file if its in the last chunk
        long endOffset = getEndOffset(timestamp);

        //length of the current index chunk
        int chunkLength = (int) (endOffset - startOffset);

        //the future that performs the actual read on completion
        final Future refineFuture = Future.future()
                                          .setHandler(done -> readDatapointFromDisk(timestamp, datapointHandler));

        //check if index refinement is needed (the chunk is still too large)
        if (chunkLength > this.chunkSize * 2) {

            if (this.refineTracker.contains(startOffset)) {
                LOG.trace("Waiting until index block {} is refined", toHexString(startOffset));
                vertx.setPeriodic(50, timerId -> {
                    if (!this.refineTracker.contains(startOffset)) {
                        vertx.cancelTimer(timerId);
                        refineFuture.complete();
                    } else {
                        LOG.debug("Still waiting for block {} to complete refinement", toHexString(startOffset));

                    }
                });

            } else {
                LOG.debug("Refining index block between {} and {}", startOffset, endOffset);
                this.refinements++;
                this.refineTracker.add(startOffset);
                final int indexPoints = chunkLength / this.chunkSize;
                final long start = System.currentTimeMillis();
                buildIndexTable(this.file, startOffset, indexChunkSize, indexPoints, this.chunkSize, subIndex -> {
                    this.index.putAll(subIndex);
                    this.refineTracker.remove(startOffset);
                    LOG.trace("Refinement of block {} finished in {} ms",
                              toHexString(startOffset),
                              System.currentTimeMillis() - start);
                    refineFuture.complete();
                });
            }

        } else {
            //no refinement needed
            refineFuture.complete();
        }

    }

    /**
     * This method performs a direct disk access to read the datapoint.
     *
     * @param timestamp
     *         the timestamp to look for
     * @param datapointHandler
     *         the handler to be notified when the datapoint has been read.
     */
    private void readDatapointFromDisk(final long timestamp, final Handler<JsonObject> datapointHandler) {

        //get the offset of the first chunk or 0 if it's before that
        long startOffset = getStartOffset(timestamp);
        //get offset of the next chunk of size of file if its in the last chunk
        long endOffset = getEndOffset(timestamp);
        int chunkLength = (int) (endOffset - startOffset);

        final long start = System.currentTimeMillis();
        file.read(Buffer.buffer(chunkLength), 0, startOffset, chunkLength, result -> {
            if (result.succeeded()) {
                final Buffer block = result.result();
                final TreeMap<Long, BitcoinDatapoint> localIndex = new TreeMap<>();

                readDatapoints(block, dp -> {
                    BitcoinDatapoint btcDp = BitcoinDatapoint.fromOffsetBuffer(dp);
                    localIndex.put(btcDp.getTimestamp(), btcDp);
                });
                LOG.debug("Processed chunk {} of size {} with {} entries in {} ms",
                          toHexString(startOffset),
                          chunkLength,
                          localIndex.size(),
                          System.currentTimeMillis() - start);
                readDatapointFromCache(timestamp, localIndex, dp -> {
                    datapointHandler.handle(toJson(dp));
                });
            } else {
                LOG.error("Could not read chunk {}", toHexString(startOffset), result.cause());
            }
        });

    }

    /**
     * Determines the file offset for the start position for reading datapoints for the given timestamp. Using the
     * index, the indexpoint that is lower and closest to the timestamp is used. If no previous indexpoint is found, 0
     * is returned, indicating the beginning of the file.
     *
     * @param timestamp
     *         the timestamp to read the fileoffset from
     *
     * @return the offset point to start reading from
     */
    private long getStartOffset(final long timestamp) {

        return Optional.ofNullable(this.index.lowerEntry(timestamp)).map(Map.Entry::getValue).orElseGet(() -> 0L);
    }

    /**
     * Determines the file offset for the end position for reading datapoints for the given timestamp. Using the index,
     * the indexpoint that is larger and closest to the timestamp is used. If no larger index point is found, the
     * filesize is used, indicating the end of the file.
     *
     * @param timestamp
     *         the timestamp to read the fileoffset from
     *
     * @return the offset point to end reading
     */
    private long getEndOffset(final long timestamp) {

        return Optional.ofNullable(this.index.ceilingEntry(timestamp))
                       .map(Map.Entry::getValue)
                       .orElseGet(() -> filesize);
    }

    /**
     * Reads the datapoint for a given timestamp from the map datastructure an notifies the handler when
     * the datapoint has been read. If the map does not contain the exact datapoint, a new datapoint is interpolated
     * from the previous lower and next bigger datapoint of the tree structure. If there are no two datapoints to
     * interpolate, the next bigger (for lower end) and previous lower (for upper end) are used.
     *
     * @param timestamp
     *         the timestamp for which to retrieve the datapoint
     * @param blockMap
     *         the map of a datablock containing the datapoints
     * @param datapointHandler
     *         the handler is notified when the datapoint is retrieved
     */
    private void readDatapointFromCache(final long timestamp,
                                        final TreeMap<Long, BitcoinDatapoint> blockMap,
                                        final Handler<BitcoinDatapoint> datapointHandler) {

        final Optional<Map.Entry<Long, BitcoinDatapoint>> ceilingEntry = Optional.ofNullable(blockMap.ceilingEntry(
                timestamp));
        final Optional<Map.Entry<Long, BitcoinDatapoint>> lowerEntry = Optional.ofNullable(blockMap.lowerEntry(timestamp));

        datapointHandler.handle(lowerEntry.map(Map.Entry::getValue)
                                          .map(le -> interpolateOrGet(timestamp, le, ceilingEntry))
                                          .orElseGet(() -> getOrFail(timestamp, ceilingEntry)));
    }

    /**
     * Interpolates datapoint if for the given lower datapoint a ceilingEntry exists, otherwise returns the
     * lower datapoint
     *
     * @param timestamp
     *         the timestamp for which the datapoint should be determined
     * @param le
     *         the lower entry for the timestamp
     * @param ceilingEntry
     *         the optional ceiling entry for the timestamp
     *
     * @return the interpolated datapoint or the lower entry
     */
    private BitcoinDatapoint interpolateOrGet(final long timestamp,
                                              final BitcoinDatapoint le,
                                              final Optional<Map.Entry<Long, BitcoinDatapoint>> ceilingEntry) {

        return ceilingEntry.map(Map.Entry::getValue)
                           .map(ce -> interpolateDatapoint(timestamp, le, ce))
                           .orElseGet(() -> le);
    }

    /**
     * Returns the ceiling datapoint or throws an exception if there is no ceiling datapoint for the given
     * timestamp. The exception case can only occur, if there are no datapoints in the file. This method should
     * be used after reading a lower datapoint without success
     *
     * @param timestamp
     *         the timestamp for which to retrieve the datapoint
     * @param ceilingEntry
     *         the next bigger entry for the datapoint
     *
     * @return the datapoint at the requested position
     */
    private BitcoinDatapoint getOrFail(final long timestamp,
                                       final Optional<Map.Entry<Long, BitcoinDatapoint>> ceilingEntry) {

        return ceilingEntry.map(Map.Entry::getValue)
                           .orElseThrow(() -> new RuntimeException("No Datapoint found for timestamp " + timestamp));
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
    private void readDatapoints(final Buffer buffer, Handler<BufferedOffsetDatapoint> handler) {

        final int len = buffer.length();
        int pos = 0;

        while (pos < len && pos != -1) {
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
    private int readNextLine(final Buffer buffer, int initialOffset, final Handler<BufferedOffsetDatapoint> handler) {

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
        final int startOffset = findLineBegin(buffer, initialOffset);
        if (startOffset == -1) {
            return buffer.length();
        }
        final int endOffset = findLineBegin(buffer, startOffset);
        if (endOffset == -1) {
            return buffer.length();
        }
        handler.handle(new BufferedOffsetDatapoint(startOffset + initialOffset,
                                                   buffer.getBuffer(startOffset, endOffset-1).getByteBuf().nioBuffer()));
        return endOffset;
    }

    /**
     * Interpolates a datapoint from two given datapoints. The method applies linear interpolation.
     *
     * @param timestamp
     *         the timestamp for which the interpolated datapoint should be created
     * @param le
     *         the lower entry
     * @param ce
     *         the ceiling entry
     *
     * @return
     */
    private BitcoinDatapoint interpolateDatapoint(final long timestamp,
                                                  final BitcoinDatapoint le,
                                                  final BitcoinDatapoint ce) {

        //calculate the proportion of the distance of the timestamp to the lower entry in relation
        //to the total interval
        float le_pc = ((float) (timestamp - le.getTimestamp()) / (ce.getTimestamp() - le.getTimestamp()));
        //calculate the remaining propportion of the distance of the timestamp to the ceiling entry in
        //relation to the total interval
        float ce_pc = 1.0f - le_pc;

        float price = le.getPrice() * le_pc + ce.getPrice() * ce_pc;
        float volume = (le.getVolume() * le_pc + ce.getVolume()) * ce_pc;

        return new BitcoinDatapoint(timestamp, price, volume);
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
        byte c = -1;
        while (pos < len && (c = buffer.getByte(pos++)) != '\n') {
            //noop
        }
        if (c != '\n') {
            return -1;
        }
        return pos;
    }

}
