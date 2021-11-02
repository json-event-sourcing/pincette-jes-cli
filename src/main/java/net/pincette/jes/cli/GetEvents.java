package net.pincette.jes.cli;

import static com.mongodb.reactivestreams.client.MongoClients.create;
import static net.pincette.jes.cli.Application.VERSION;
import static net.pincette.jes.cli.Util.print;
import static net.pincette.jes.cli.Util.toArray;
import static net.pincette.jes.util.Mongo.events;
import static net.pincette.util.Util.tryToDoWithRethrow;

import net.pincette.jes.util.Mongo.DbContext;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;

@Command(
    name = "get",
    version = VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description =
        "Gets all the events of an aggregate instance in chronological order and writes them as a "
            + "JSON array to the terminal.")
class GetEvents extends AggregateCommand implements Runnable {
  @Option(
      names = {"-i", "--id"},
      required = true,
      description = "The identifier of an aggregate instance.")
  private String id;

  public void run() {
    tryToDoWithRethrow(
        () -> create(mongoUrl),
        client ->
            print(
                toArray(
                    events(
                        id,
                        aggregate,
                        environment,
                        new DbContext(client.getDatabase(mongoDatabase), null)))));
  }
}
