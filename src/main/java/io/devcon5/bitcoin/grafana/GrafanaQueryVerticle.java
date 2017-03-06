package io.devcon5.bitcoin.grafana;

import static io.devcon5.util.JsonFactory.toJsonArray;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.devcon5.grafana.Range;
import io.devcon5.grafana.RangeParser;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;

/**
 *
 */
public class GrafanaQueryVerticle extends AbstractVerticle {

    private static final Logger LOG = getLogger(GrafanaQueryVerticle.class);

    private RangeParser rangeParser;

    @Override
    public void start() throws Exception {

        JsonObject config = Vertx.currentContext().config();
        this.rangeParser = new RangeParser();

        vertx.eventBus().consumer("query", request -> {
            JsonObject query = (JsonObject) request.body();

            final Range range = parseRange(query);
            int maxDatapoints = query.getInteger("maxDataPoints", 1);
            long interval = range.getDuration() / maxDatapoints;

            CompositeFuture.all(IntStream.range(0, maxDatapoints)
                                         .mapToLong(i -> range.getStart() + i * interval)
                                         .mapToObj(ts -> {
                                             final Future<JsonObject> datapoint = Future.future();
                                             vertx.eventBus()
                                                  .send("bitcoinPrice",
                                                        ts,
                                                        btcResult -> {
                                                            if(btcResult.succeeded()) {
                                                                datapoint.complete((JsonObject) btcResult.result().body());
                                                            } else {
                                                                LOG.error("Could not retrieve datapoint for ts={}",ts, btcResult.cause());
                                                            }

                                                        });
                                             return datapoint;
                                         })
                                         .collect(Collectors.toList())).setHandler(datapoints -> {
                if(datapoints.succeeded()) {
                    LOG.info("Returning response with {} datapoints", datapoints.result().list());
                    request.reply(datapoints.result()
                                            .list()
                                            .stream()
                                            .map(o -> (JsonObject) o)
                                            .sorted((j1, j2) -> j1.getLong("ts").compareTo(j2.getLong("ts")))
                                            .map(jo -> new JsonArray().add(jo.getDouble("price")).add(jo.getLong("ts")))
                                            .collect(toJsonArray()));
                } else {
                    LOG.error("Query failed", datapoints.cause());
                }
            });

        });

    }

    private Range parseRange(final JsonObject query) {

        final JsonObject jsonRange = query.getJsonObject("range");
        return this.rangeParser.parse(jsonRange.getString("from"), jsonRange.getString("to"));
    }
}
