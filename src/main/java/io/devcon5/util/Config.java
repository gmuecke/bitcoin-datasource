package io.devcon5.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import io.vertx.core.json.JsonObject;

/**
 * Reads a JSON file into a json object. The method is blocking and intended to be used for bootstrapping a vertx
 * service when starting programmatically. When a Verticle is deployed via vertx-cli, you should use the
 * --config option instead.
 */
public class Config {

    public static JsonObject fromFile(String file){

        try {
            return new JsonObject(new String(Files.readAllBytes(Paths.get(file))));
        } catch (IOException e) {
            throw new RuntimeException("Could not read file " + file, e);
        }
    }
}
