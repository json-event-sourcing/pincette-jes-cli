package net.pincette.jes.cli;

import static net.pincette.jes.cli.Application.VERSION;
import static net.pincette.jes.cli.Util.loadProperties;
import static net.pincette.jes.util.Kafka.wrap;
import static net.pincette.util.Util.tryToDoWithRethrow;
import static org.apache.kafka.clients.admin.Admin.create;

import java.io.File;
import java.util.TreeSet;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;

@Command(
    name = "list",
    version = VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description = "Lists the Kafka topics.")
class ListTopics implements Runnable {
  @Option(
      names = {"-c", "--config-file"},
      required = true,
      description = "A Kafka configuration file.")
  private File config;

  @SuppressWarnings("java:S106") // Not logging.
  public void run() {
    tryToDoWithRethrow(
        () -> create(loadProperties(config)),
        admin ->
            wrap(admin.listTopics().names())
                .thenApply(TreeSet::new)
                .toCompletableFuture()
                .join()
                .forEach(System.out::println));
  }
}
