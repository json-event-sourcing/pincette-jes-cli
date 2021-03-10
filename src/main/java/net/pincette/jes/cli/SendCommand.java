package net.pincette.jes.cli;

import static com.mongodb.reactivestreams.client.MongoClients.create;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static net.pincette.jes.cli.Application.VERSION;
import static net.pincette.jes.cli.Util.sendJson;
import static net.pincette.jes.util.JsonFields.CORR;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.JsonFields.JWT;
import static net.pincette.jes.util.JsonFields.SUB;
import static net.pincette.jes.util.JsonFields.TYPE;
import static net.pincette.json.Jslt.reader;
import static net.pincette.json.Jslt.transformerObject;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.mongo.JsonClient.findPublisher;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Util.join;

import com.mongodb.reactivestreams.client.MongoClient;
import com.schibsted.spt.data.jslt.impl.FileSystemResourceResolver;
import java.io.File;
import java.util.concurrent.CompletionStage;
import java.util.function.UnaryOperator;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.Jslt.Context;
import net.pincette.json.JsonUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;

@Command(
    name = "send",
    version = VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description = "Queries an aggregate collection and sends commands created from the results.")
class SendCommand extends AggregateCommand implements Runnable {
  @Option(
      names = {"-c", "--config-file"},
      required = true,
      description = "A ccloud configuration file.")
  private File config;

  @Option(
      names = {"-j", "--jslt"},
      required = true,
      description =
          "The JSLT script that transforms an aggregate instance to a command. It only "
              + "has to generate the \"_command\" field and optionally some data. The user will be "
              + "set to \"system\".")
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
        transformerObject(
            new Context(reader(jslt))
                .withResolver(
                    new FileSystemResourceResolver(jslt.getAbsoluteFile().getParentFile())));

    try (final MongoClient client = create(mongoUrl);
        final KafkaProducer<String, JsonObject> producer = Util.producer(config)) {
      join(
          with(findPublisher(
                  client.getDatabase(mongoDatabase).getCollection(collection()),
                  ofNullable(query)
                      .flatMap(JsonUtil::from)
                      .filter(JsonUtil::isObject)
                      .map(JsonValue::asJsonObject)
                      .orElse(null)))
              .mapAsync(aggregate -> sendCommand(aggregate, producer, transformer))
              .get());
    }
  }

  private CompletionStage<Boolean> sendCommand(
      final JsonObject aggregate,
      final KafkaProducer<String, JsonObject> producer,
      final UnaryOperator<JsonObject> transformer) {
    return sendJson(producer, topic(aggregate), command(aggregate, transformer));
  }

  private String suffix() {
    return environment != null ? ("-" + environment) : "";
  }

  private String topic(final JsonObject aggregate) {
    return aggregate.getString(TYPE) + "-command" + suffix();
  }
}
