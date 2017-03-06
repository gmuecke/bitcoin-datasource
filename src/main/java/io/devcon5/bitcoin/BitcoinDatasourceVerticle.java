package io.devcon5.bitcoin;

import io.devcon5.bitcoin.vertx.util.Config;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

/**
 * This the main verticle for the datasource
 */
public class BitcoinDatasourceVerticle extends AbstractVerticle {

    /**
     * Main method for development purposes only. Use CLI to start/deploy verticle for production use.
     * @param args
     */
    public static void main(String... args) {

        final Vertx vertx = Vertx.vertx();
        final JsonObject config = Config.fromFile("config/config.json");
        vertx.deployVerticle(BitcoinDatasourceVerticle.class.getName(), new DeploymentOptions().setConfig(config));
    }

    @Override
    public void start() throws Exception {

        final JsonObject config = Vertx.currentContext().config();
        vertx.deployVerticle(BitcoinHistoryDataVerticle.class.getName(), new DeploymentOptions().setConfig(config));
        vertx.deployVerticle(HttpServerVerticle.class.getName(), new DeploymentOptions().setConfig(config));
    }
}
