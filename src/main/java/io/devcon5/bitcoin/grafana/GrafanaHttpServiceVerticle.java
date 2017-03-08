package io.devcon5.bitcoin.grafana;

import static org.slf4j.LoggerFactory.getLogger;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;

/**
 *
 */
public class GrafanaHttpServiceVerticle extends AbstractVerticle{

    private static final Logger LOG = getLogger(GrafanaHttpServiceVerticle.class);

    @Override
    public void start(final Future<Void> startFuture) throws Exception {

        JsonObject config = Vertx.currentContext().config();
        int port = config.getInteger("http.port", 11011);

        //initialize the http service
        final Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        router.route("/").handler(this::ping);
        router.options("/*").handler(this::options);

        router.post("/*").handler(ctx -> {
            LOG.info("POST {}\n{}", ctx.normalisedPath(), ctx.getBody().toJsonObject());
            ctx.next();
        });
        router.post("/search").handler(this::handleSearch);
        router.post("/annotations").handler(this::handleAnnotations);
        router.post("/query").handler(this::handleQuery);

        vertx.createHttpServer().requestHandler(router::accept).listen(port, result -> {
            if(result.succeeded()){
                LOG.info("HTTP service running on port {}", port);
            } else {
                LOG.error("HTTP startup failed", result.cause());
            }
        });
    }

    private void handleQuery(final RoutingContext ctx) {
        vertx.eventBus().send("query", ctx.getBodyAsJson(), reply -> {
            if(reply.succeeded()){
                JsonArray resultSet = ((JsonArray)reply.result().body());
                JsonArray result = new JsonArray().add(new JsonObject().put("target", "USD").put("datapoints", resultSet));
                LOG.debug("Sending Response with {} datapoints: {}", resultSet.size(), result.encodePrettily());
                ctx.response().putHeader("content-type", "application/json").end(result.encode());
            } else {
                ctx.response().setStatusCode(500).setStatusMessage("Could not process request" + reply.cause().getMessage());
            }
        });
    }

    private void handleAnnotations(final RoutingContext ctx) {
        ctx.response().putHeader("content-type", "application/json").end("[]");
    }

    private void handleSearch(final RoutingContext ctx) {
        ctx.response().putHeader("content-type", "application/json").end("[\"USD\"]");

    }

    private void options(RoutingContext ctx) {

        ctx.response()
           .putHeader("Access-Control-Allow-Headers", "accept, content-type")
           .putHeader("Access-Control-Allow-Methods", "POST")
           .putHeader("Access-Control-Allow-Origin", "*")
           .end();
    }

    private void ping(RoutingContext routingContext) {

        routingContext.response().putHeader("content-type", "text/html").end("Grafana Bitcoin History Datasource " + System.currentTimeMillis());
    }
}
