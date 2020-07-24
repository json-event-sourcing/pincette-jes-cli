package net.pincette.jes.cli;

import static com.mongodb.reactivestreams.client.MongoClients.create;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static net.pincette.jes.cli.Application.VERSION;
import static net.pincette.jes.cli.Util.fromProperties;
import static net.pincette.jes.cli.Util.loadProperties;
import static net.pincette.jes.util.JsonFields.CORR;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.JsonFields.JWT;
import static net.pincette.jes.util.JsonFields.SUB;
import static net.pincette.jes.util.JsonFields.TYPE;
import static net.pincette.jes.util.Kafka.createReliableProducer;
import static net.pincette.jes.util.Kafka.send;
import static net.pincette.json.Jslt.transformer;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.mongo.JsonClient.findPublisher;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Util.join;
import static net.pincette.util.Util.must;

import com.mongodb.reactivestreams.client.MongoClient;
import com.schibsted.spt.data.jslt.impl.FileSystemResourceResolver;
import java.io.File;
import java.util.concurrent.CompletionStage;
import java.util.function.UnaryOperator;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.jes.util.JsonSerializer;
import net.pincette.json.JsonUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;

@Command(
    name = "send",
    version = VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description = "Queries an aggregate collection and send commands created from the results")
class SendCommand extends AggregateCommand implements Runnable {
  @Option(
      names = {"-c", "--config-file"},
      required = true,
      description = "A ccloud configuration file")
  private File config;

  @Option(
      names = {"-j", "--jslt"},
      required = true,
      description =
          "The JSLT script that transforms an aggregate instance to a command. It only "
              + "has to generate the \"_command\" field and optionally some data. The user will be "
              + "set to \"system\"")
  private File jslt;

  @Option(
      names = {"-q", "--query"},
      description =
          "A MongoDB query to select the aggregate instances. Without it all instances "
              + "will receive the command.")
  private String query;

  private static JsonObject command(
      final JsonObject aggregate, final UnaryOperator<JsonObject> transformer) {
    return createObjectBuilder(transformer.apply(aggregate))
        .add(ID, aggregate.getString(ID))
        .add(TYPE, aggregate.getString(TYPE))
        .add(JWT, createObjectBuilder().add(SUB, "system"))
        .add(CORR, randomUUID().toString())
        .build();
  }

  private String collection() {
    return aggregate + suffix();
  }

  public void run() {
    final UnaryOperator<JsonObject> transformer =
        transformer(
            jslt,
            null,
            null,
            new FileSystemResourceResolver(jslt.getAbsoluteFile().getParentFile()));

    try (final MongoClient client = create(mongoUrl);
        final KafkaProducer<String, JsonObject> producer =
            createReliableProducer(
                fromProperties(loadProperties(config)),
                new StringSerializer(),
                new JsonSerializer())) {
      join(
          with(findPublisher(
                  client.getDatabase(mongoDatabase).getCollection(collection()),
                  ofNullable(query)
                      .flatMap(JsonUtil::from)
                      .filter(JsonUtil::isObject)
                      .map(JsonValue::asJsonObject)
                      .orElse(null)))
              .map(aggregate -> sendCommand(aggregate, producer, transformer))
              .async()
              .get());
    }
  }

  private CompletionStage<Boolean> sendCommand(
      final JsonObject aggregate,
      final KafkaProducer<String, JsonObject> producer,
      final UnaryOperator<JsonObject> transformer) {
    return send(
            producer,
            new ProducerRecord<>(
                topic(aggregate), aggregate.getString(ID), command(aggregate, transformer)))
        .thenApply(result -> must(result, r -> r));
  }

  private String suffix() {
    return environment != null ? ("-" + environment) : "";
  }

  private String topic(final JsonObject aggregate) {
    return aggregate.getString(TYPE) + "-command" + suffix();
  }
}
