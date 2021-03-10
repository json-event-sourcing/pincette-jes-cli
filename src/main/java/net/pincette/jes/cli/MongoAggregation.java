package net.pincette.jes.cli;

import static com.mongodb.reactivestreams.client.MongoClients.create;
import static net.pincette.json.Jslt.reader;
import static net.pincette.json.Jslt.transformerObject;
import static net.pincette.json.JsltCustom.customFunctions;
import static net.pincette.json.JsonUtil.createReader;
import static net.pincette.mongo.JsonClient.aggregationPublisher;
import static net.pincette.mongo.JsonClient.findPublisher;
import static net.pincette.rs.Chain.with;
import static net.pincette.util.Util.tryToGetWithRethrow;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import java.io.File;
import java.io.FileInputStream;
import java.util.function.UnaryOperator;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import net.pincette.json.Jslt.Context;
import net.pincette.json.JsonUtil;
import net.pincette.rs.PassThrough;
import org.bson.Document;
import org.reactivestreams.Publisher;
import picocli.CommandLine.Option;

class MongoAggregation extends MongoCommand {
  @Option(
      names = {"-a", "--aggregation"},
      description = "The aggregation pipeline file that will be run on the MongoDB collection.")
  private File aggregation;

  @Option(
      names = {"-col", "--collection"},
      required = true,
      description =
          "The MongoDB collection that will be queried. Without an an aggregation all "
              + "documents are returned.")
  private String collection;

  @Option(
      names = {"-j", "--jslt"},
      description = "The JSLT script that transforms the result.")
  private File jslt;

  protected Publisher<JsonObject> aggregate() {
    final MongoClient client = create(mongoUrl);
    final UnaryOperator<JsonObject> transformer =
        jslt != null
            ? transformerObject(new Context(reader(jslt)).withFunctions(customFunctions()))
            : j -> j;
    return with(aggregation != null
            ? aggregationPublisher(collection(client), readAggregation())
            : findPublisher(collection(client)))
        .map(transformer)
        .map(
            new PassThrough<JsonObject>() {
              @Override
              public void onComplete() {
                super.onComplete();
                client.close();
              }
            })
        .get();
  }

  protected MongoCollection<Document> collection(final MongoClient client) {
    return client.getDatabase(mongoDatabase).getCollection(collection);
  }

  private JsonArray readAggregation() {
    return tryToGetWithRethrow(
            () -> createReader(new FileInputStream(aggregation)), JsonReader::readArray)
        .orElseGet(JsonUtil::emptyArray);
  }
}
