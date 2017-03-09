package io.devcon5.bitcoin;

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

import io.devcon5.util.LRUCache;
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

    private static final BitcoinDatapoint DATA_PLACEHOLDER = new BitcoinDatapoint(-1,
                                                                                  Buffer.buffer("-1,-1,-1\n"),
                                                                                  2,
                                                                                  5);
    private static final Buffer BLOCK_PLACEHOLDER = Buffer.buffer("PLACEHOLDER");

    private LRUCache<Long, BitcoinDatapoint> dataCache;
    private LRUCache<Long, Buffer> blockCache;
    private TreeMap<Long, Long> index = new TreeMap<>();
    private Set<Long> refineTracker = new HashSet<>();

    //file stats
    private String filename;
    private long filesize;
    private int lineLength;
    private AsyncFile file;
    //index chunk size
    private int chunkSize;
    private long refinements = 0;


    @Override
    public void start(final Future<Void> startFuture) throws Exception {

        final JsonObject config = Vertx.currentContext().config();

        this.filename = config.getString("bitcoinFile");
        this.filesize = Files.size(Paths.get(this.filename));
        this.lineLength = config.getInteger("lineLength");
        this.chunkSize = config.getInteger("chunkSize");

        LOG.debug("Bitcoin datafile: {} bytes, {} chunks, {} bytes chunkSize",
                  filesize,
                  (filesize / chunkSize),
                  chunkSize);

        //create an index for fast-lookups
        vertx.fileSystem().open(this.filename, new OpenOptions(), openFile -> createPartialIndex(openFile, result -> {
            this.index = result;
            LOG.info("Created partial index with {} datapoints", this.index.size());
            startFuture.complete();
        }));

        vertx.eventBus().consumer("bitcoinPrice", req -> readDatapoint((Long) req.body(), req::reply));

        initCache(config);
    }

    @Override
    public void stop() throws Exception {

        this.file.close();
    }

    private void initCache(final JsonObject config) {

        final JsonObject cacheConfig = config.getJsonObject("cache");
        final int cacheSize = cacheConfig.getInteger("cacheSize", 1024);
        LOG.debug("Creating cache for {} entries", cacheSize);

        this.blockCache = new LRUCache<>(cacheSize);
        this.dataCache = new LRUCache<>(cacheSize * 16);

        final int statsInterval = cacheConfig.getInteger("statsInterval", 60000);
        vertx.setPeriodic(statsInterval, t -> {

            LOG.info("Cache statistics: \n blockCache: {} \n dataCache: {}",
                     blockCache.getStatistics(), dataCache.getStatistics());
        });
        vertx.setPeriodic(statsInterval, t -> {

            LOG.info("Index statistics: \n entries={}, refinements={}, blocksInRefinement={}",
                     this.index.size(),
                     this.refinements,
                     this.refineTracker.size());
        });
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
            buildIndexTable(openFile.result(), 0, lineLength * 2, indexPoints, (this.chunkSize * 16), indexHandler);
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
                                          .setHandler(done -> readDatapointDisk(timestamp, datapointHandler));

        //check if index refinement is needed (the chunk is still too large)
        if (chunkLength > this.chunkSize * 2) {

            if (this.refineTracker.contains(startOffset)) {
                LOG.trace("Waiting until index block {} is refined", startOffset);
                vertx.setPeriodic(50, timerId -> {
                    if (!this.refineTracker.contains(startOffset)) {
                        vertx.cancelTimer(timerId);
                        refineFuture.complete();
                    } else {
                        LOG.debug("Still waiting for startOffset {} to complete refinement", startOffset);

                    }
                });

            } else {
                LOG.debug("Refining index block between {} and {}", startOffset, endOffset);
                this.refinements++;
                this.refineTracker.add(startOffset);
                final int indexPoints = chunkLength / this.chunkSize;

                buildIndexTable(this.file, startOffset, lineLength * 2, indexPoints, this.chunkSize, subIndex -> {
                    this.index.putAll(subIndex);
                    this.refineTracker.remove(startOffset);
                    refineFuture.complete();
                });
            }

        } else {
            refineFuture.complete();
        }

    }

    /**
     * This method performs a direct disk access to read the datapoint.
     * @param timestamp
     *  the timestamp to look for
     * @param datapointHandler
     *  the handler to be notified when the datapoint has been read.
     */
    private void readDatapointDisk(final long timestamp, final Handler<JsonObject> datapointHandler) {

        //get the offset of the first chunk or 0 if it's before that
        long startOffset = getStartOffset(timestamp);
        //get offset of the next chunk of size of file if its in the last chunk
        long endOffset = getEndOffset(timestamp);
        int chunkLength = (int) (endOffset - startOffset);

        file.read(Buffer.buffer(chunkLength), 0, startOffset, chunkLength, result -> {
            if (result.succeeded()) {
                LOG.trace("Reading chunk of size {} from offset {}", chunkLength, startOffset);
                final Buffer block = result.result();
                final TreeMap<Long, BitcoinDatapoint> localIndex = new TreeMap<>();
                readDatapoints(block, dp -> localIndex.put(dp.getTimestamp(), dp));
                LOG.trace("Processed chunk with {} entries", localIndex.size());
                readDatapointFromCache(timestamp, localIndex, dp -> {
                    datapointHandler.handle(dp.toJson());
                });
            } else {
                LOG.error("Could not read chunk from offset {}", startOffset, result.cause());
            }
        });

    }

    private void readDatapointFromCache(final long timestamp, Handler<JsonObject> datapointHandler) {

        LOG.trace("Reading datapoint at {}", timestamp);

        //define what to do when we have a matching datapoint
        final long start = System.currentTimeMillis();
        final Future<BitcoinDatapoint> readFuture = Future.<BitcoinDatapoint>future().setHandler(result -> {
            if (result.succeeded()) {
                BitcoinDatapoint dp = result.result();
                if(dp != null) {
                    LOG.trace("Retrieved datapoint {}->{} for in {} ms", timestamp, result.result(), System.currentTimeMillis()-start);
                    datapointHandler.handle(result.result().toJson());
                } else {
                    LOG.warn("Datapoint for ts {} has already been removed from cache", timestamp);
                    datapointHandler.handle(DATA_PLACEHOLDER.toJson());
                }

            } else {
                LOG.error("Can not read datapoint for ts={}", timestamp, result.cause());
            }
        });

        final BitcoinDatapoint datapoint = this.dataCache.putIfAbsent(timestamp, DATA_PLACEHOLDER);
        if (datapoint == null) {

            //datapoint is unknown
            readBlockFromCache(timestamp, result -> {
                if (result.succeeded()) {

                    Buffer buf = result.result();
                    if(buf == null){
                        LOG.warn("Buffer has alread been removed from cache");
                        readFuture.complete(DATA_PLACEHOLDER);
                    } else {
                        final TreeMap<Long, BitcoinDatapoint> localIndex = new TreeMap<>();
                        readDatapoints(buf, dp -> localIndex.put(dp.getTimestamp(), dp));
                        LOG.trace("Processed chunk with {} entries", localIndex.size());
                        readDatapointFromCache(timestamp, localIndex, dp -> {
                            this.dataCache.put(timestamp, dp);
                            readFuture.complete(dp);
                        });
                    }
                    //TODO check if its feasible to read the entire chunk into the dp cache
                }
            });

        } else if (datapoint == DATA_PLACEHOLDER) {
            //wait until datapoint is read
            vertx.setPeriodic(10, timerId -> {
                BitcoinDatapoint dp = this.dataCache.get(timestamp);
                if (dp != DATA_PLACEHOLDER) {
                    vertx.cancelTimer(timerId);
                    readFuture.complete(dp);
                } else {
                    LOG.debug("Still waiting for datapoint for ts {} to be read",timestamp );

                }
            });
        } else {
            readFuture.complete(datapoint);
        }

    }

    private void readBlockFromCache(final long timestamp, final Handler<AsyncResult<Buffer>> handler) {

        //get the offset of the first chunk or 0 if it's before that
        final long startOffset = getStartOffset(timestamp);
        //get offset of the next chunk of size of file if its in the last chunk
        final long endOffset = getEndOffset(timestamp);
        final int chunkLength = (int) (endOffset - startOffset);

        //define what to do on obtaining a block of data
        final Future<Buffer> chunkFuture = Future.<Buffer>future().setHandler(handler);

        //now obtain the block of data
        final Buffer cacheBlock = this.blockCache.putIfAbsent(startOffset, BLOCK_PLACEHOLDER);
        if (cacheBlock == null) {
            file.read(Buffer.buffer(chunkLength), 0, startOffset, chunkLength, result -> {
                if (result.succeeded()) {
                    LOG.trace("Reading chunk of size {} from offset {}", chunkLength, startOffset);
                    final Buffer block = result.result();
                    this.blockCache.put(startOffset, block);
                    chunkFuture.complete(block);
                } else {
                    LOG.error("Could not read chunk from offset {}", startOffset, result.cause());
                }
            });

        } else if (cacheBlock == BLOCK_PLACEHOLDER) {
            //wait until placeholder is replaced
            vertx.setPeriodic(500, timerId -> {
                final Buffer block = this.blockCache.get(startOffset);
                if (block != BLOCK_PLACEHOLDER) {
                    vertx.cancelTimer(timerId);
                    chunkFuture.complete(block);
                } else {
                    LOG.debug("Still waiting for chunk {} to be read", startOffset );
                }
            });
        } else {
            chunkFuture.complete(cacheBlock);
        }
    }

    private Long getStartOffset(final long timestamp) {

        return Optional.ofNullable(this.index.lowerEntry(timestamp)).map(Map.Entry::getValue).orElseGet(() -> 0L);
    }

    private Long getEndOffset(final long timestamp) {

        return Optional.ofNullable(this.index.ceilingEntry(timestamp))
                       .map(Map.Entry::getValue)
                       .orElseGet(() -> filesize);
    }

    private void readDatapointFromCache(final long timestamp,
                                        final TreeMap<Long, BitcoinDatapoint> cacheBlock,
                                        final Handler<BitcoinDatapoint> datapointHandler) {

        final Optional<Map.Entry<Long, BitcoinDatapoint>> ceilingEntry = Optional.ofNullable(cacheBlock.ceilingEntry(
                timestamp));
        final Optional<Map.Entry<Long, BitcoinDatapoint>> lowerEntry = Optional.ofNullable(cacheBlock.lowerEntry(
                timestamp));

        datapointHandler.handle(lowerEntry.map(Map.Entry::getValue)
                                          .map(le -> interpolateOrGet(timestamp, le, ceilingEntry))
                                          .orElseGet(() -> getOrFail(timestamp, ceilingEntry)));
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

    private BitcoinDatapoint interpolateOrGet(final long timestamp,
                                              final BitcoinDatapoint le,
                                              final Optional<Map.Entry<Long, BitcoinDatapoint>> ceilingEntry) {

        return ceilingEntry.map(Map.Entry::getValue).map(ce -> mergeDatapoints(timestamp, le, ce)).orElseGet(() -> le);
    }

    private BitcoinDatapoint getOrFail(final long timestamp,
                                       final Optional<Map.Entry<Long, BitcoinDatapoint>> ceilingEntry) {

        return ceilingEntry.map(Map.Entry::getValue)
                           .orElseThrow(() -> new RuntimeException("No Datapoint found for timestamp " + timestamp));
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

    private BitcoinDatapoint mergeDatapoints(final long timestamp,
                                             final BitcoinDatapoint le,
                                             final BitcoinDatapoint ce) {
        String ts = String.valueOf(timestamp);
        String pr = String.valueOf((le.getPrice() + ce.getPrice()) / 2);
        String vol = String.valueOf((le.getVolume() + ce.getVolume()) / 2);

        return new BitcoinDatapoint(-1L,
                                    Buffer.buffer(43)
                                          .appendString(ts)
                                          .appendString(",")
                                          .appendString(pr)
                                          .appendString(",")
                                          .appendString(vol),
                                    ts.length(),
                                    ts.length() + pr.length() + 1);
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
