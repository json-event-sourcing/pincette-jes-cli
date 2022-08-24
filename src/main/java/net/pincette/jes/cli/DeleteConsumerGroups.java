package net.pincette.jes.cli;

import static net.pincette.jes.cli.Application.VERSION;
import static net.pincette.jes.cli.Util.commaSeparated;
import static net.pincette.jes.cli.Util.loadProperties;
import static net.pincette.util.Util.tryToDoWithRethrow;
import static org.apache.kafka.clients.admin.Admin.create;

import java.io.File;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;

@Command(
    name = "delete",
    version = VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description = "Deletes the Kafka consumer groups.")
class DeleteConsumerGroups implements Runnable {
  @Option(
      names = {"-c", "--config-file"},
      required = true,
      description = "A Kafka configuration file.")
  private File config;

  @Option(
      names = {"-g", "--groups"},
      required = true,
      description =
          "Deletes the given consumer groups. This is a comma-separated list of group IDs.")
  private String groups;

  @SuppressWarnings("java:S106") // Not logging.
  public void run() {
    tryToDoWithRethrow(
        () -> create(loadProperties(config)),
        admin ->
            admin
                .deleteConsumerGroups(commaSeparated(groups))
                .all()
                .toCompletionStage()
                .toCompletableFuture()
                .join());
  }
}
