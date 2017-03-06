package io.devcon5.bitcoin;

import static org.slf4j.LoggerFactory.getLogger;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import org.slf4j.Logger;

/**
 * The HTTP endpoint for querying for bitcoin prices.
 */
public class HttpServerVerticle extends AbstractVerticle {

    private static final Logger LOG = getLogger(HttpServerVerticle.class);

    @Override
    public void start(final Future<Void> startFuture) throws Exception {

        JsonObject config = Vertx.currentContext().config();
        //initialize the http service
        final Router router = Router.router(vertx);
        router.get("/bitcoin").handler(ctx -> {
            long start = System.currentTimeMillis();
            Long ts = Long.parseLong(ctx.request().getParam("ts"));
            vertx.eventBus().send("bitcoinPrice", ts, resp -> {
                JsonObject result = (JsonObject) resp.result().body();
                ctx.response().end( result.encodePrettily());
                LOG.debug("Request processed in {} ms", System.currentTimeMillis() - start );

            });
        });

        vertx.createHttpServer().requestHandler(router::accept).listen(11011, result -> {
            if(result.succeeded()){
                //TODO print out port
                LOG.info("HTTP service running");
            } else {
                LOG.error("HTTP startup failed", result.cause());
            }
        });
    }
}
