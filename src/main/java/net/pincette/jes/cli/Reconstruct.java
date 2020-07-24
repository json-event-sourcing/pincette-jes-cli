package net.pincette.jes.cli;

import static com.mongodb.reactivestreams.client.MongoClients.create;
import static java.util.Optional.ofNullable;
import static net.pincette.jes.cli.Application.VERSION;
import static net.pincette.jes.cli.Util.print;
import static net.pincette.jes.util.Mongo.events;
import static net.pincette.jes.util.Mongo.reconstructionPublisher;
import static net.pincette.rs.Chain.with;
import static net.pincette.util.Util.tryToDoWithRethrow;

import java.util.function.Predicate;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.mongo.Match;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;

@Command(
    name = "reconstruct",
    version = VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description =
        "Reconstructs an aggregate instance from its event log and writes it to the terminal")
class Reconstruct extends AggregateCommand implements Runnable {
  @Option(
      names = {"-i", "--id"},
      required = true,
      description = "The identifier of an aggregate instance")
  String id;

  @Option(
      names = {"-u", "--until"},
      description =
          "A MongoDB query, which stops the reconstruction when the intermediate result"
              + " matches it")
  private String query;

  @SuppressWarnings("java:S106") // Not logging.
  public void run() {
    final Predicate<JsonObject> predicate =
        ofNullable(query)
            .flatMap(JsonUtil::from)
            .filter(JsonUtil::isObject)
            .map(JsonValue::asJsonObject)
            .map(Match::predicate)
            .orElse(json -> false);

    tryToDoWithRethrow(
        () -> create(mongoUrl),
        client ->
            print(
                with(reconstructionPublisher(
                        events(id, aggregate, environment, client.getDatabase(mongoDatabase))))
                    .until(predicate)
                    .last()
                    .map(JsonUtil::string)
                    .get()));
  }
}
