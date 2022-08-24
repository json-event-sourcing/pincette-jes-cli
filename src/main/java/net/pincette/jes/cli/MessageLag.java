package net.pincette.jes.cli;

import static net.pincette.jes.cli.Application.VERSION;
import static net.pincette.jes.cli.Util.commaSeparated;
import static net.pincette.jes.cli.Util.loadProperties;
import static net.pincette.jes.util.Kafka.messageLag;
import static net.pincette.util.Util.tryToDoWithRethrow;
import static org.apache.kafka.clients.admin.Admin.create;

import java.io.File;
import java.util.Set;
import net.pincette.jes.util.Kafka;
import net.pincette.json.JsonUtil;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;

@Command(
    name = "lag",
    version = VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description =
        "Writes a JSON object to the terminal with all the message lags of all the "
            + "non-internal Kafka topics.")
class MessageLag implements Runnable {
  @Option(
      names = {"-c", "--config-file"},
      required = true,
      description = "A Kafka configuration file.")
  private File config;

  @Option(
      names = {"-g", "--groups"},
      description =
          "Limits the output to the given consumer groups. This is a comma-separated "
              + "list of group IDs.")
  private String groups;

  @SuppressWarnings("java:S106") // Not logging.
  public void run() {
    final Set<String> limited = groups != null ? commaSeparated(groups) : null;

    tryToDoWithRethrow(
        () -> create(loadProperties(config)),
        admin ->
            System.out.println(
                messageLag(admin, group -> limited == null || limited.contains(group))
                    .thenApply(Kafka::toJson)
                    .thenApply(JsonUtil::string)
                    .toCompletableFuture()
                    .join()));
  }
}
