package net.pincette.jes.cli;

import static com.mongodb.reactivestreams.client.MongoClients.create;
import static java.util.logging.Level.INFO;
import static java.util.logging.Logger.getLogger;
import static net.pincette.jes.cli.Application.VERSION;
import static net.pincette.jes.util.Mongo.upgradeEventLog;
import static net.pincette.util.Util.tryToDoWithRethrow;

import java.util.logging.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

@Command(
    name = "upgrade",
    version = VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description =
        "Upgrades an event collection to the new format, where the _id field is an object with "
            + "the fields id and seq.")
public class UpgradeEvents extends AggregateCommand implements Runnable {
  @SuppressWarnings("java:S106") // Not logging.
  public void run() {
    final Logger logger = getLogger("pincette-jes-cli");

    System.out.println("This may take a while. If it is interrupted, it will resume next time.");

    tryToDoWithRethrow(
        () -> create(mongoUrl),
        client ->
            upgradeEventLog(
                    aggregate,
                    environment,
                    client.getDatabase(mongoDatabase),
                    count -> {
                      if (count % 10000 == 0) {
                        logger.log(INFO, "Processed {0} events.", count);
                      }
                    })
                .toCompletableFuture()
                .join());
  }
}
