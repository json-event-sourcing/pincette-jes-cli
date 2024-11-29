package net.pincette.jes.cli;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;
import static net.pincette.jes.cli.Application.VERSION;
import static net.pincette.jes.cli.Util.commaSeparated;
import static net.pincette.jes.cli.Util.loadProperties;
import static net.pincette.util.Util.tryToDoWithRethrow;
import static org.apache.kafka.clients.admin.Admin.create;

import java.io.File;
import java.io.PrintStream;
import java.util.Set;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.common.TopicPartition;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;

@Command(
    name = "list",
    version = VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description = "Lists the Kafka consumer groups.")
class ListConsumerGroups implements Runnable {
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

  @Option(
      names = {"-s", "--short"},
      description = "Displays only the number of members of the consumer groups.")
  private boolean shortDisplay;

  private static void print(final ConsumerGroupDescription group, final PrintStream out) {
    out.println("Group ID: " + group.groupId());
    out.println("State: " + group.state());
    out.println("Members:");
    group
        .members()
        .forEach(
            m -> {
              out.println("  Client ID: " + m.clientId());
              out.println("  Consumer ID: " + m.consumerId());
              out.println("  Group instance ID: " + m.groupInstanceId().orElse(""));
              out.println("  Host: " + m.host());
              out.println(
                  "  Assigned partitions: "
                      + m.assignment().topicPartitions().stream()
                          .map(TopicPartition::partition)
                          .collect(toSet())
                          .stream()
                          .map(String::valueOf)
                          .sorted()
                          .collect(joining(",")));
            });
  }

  private static void printShort(final ConsumerGroupDescription group, final PrintStream out) {
    out.println(group.groupId() + ": " + group.members().size());
  }

  @SuppressWarnings("java:S106") // Not logging.
  public void run() {
    final Set<String> limited = groups != null ? commaSeparated(groups) : null;

    tryToDoWithRethrow(
        () -> create(loadProperties(config)),
        admin ->
            admin
                .listConsumerGroups()
                .all()
                .toCompletionStage()
                .thenApply(
                    g ->
                        g.stream()
                            .map(ConsumerGroupListing::groupId)
                            .filter(id -> limited == null || limited.contains(id))
                            .toList())
                .thenComposeAsync(
                    groupIds -> admin.describeConsumerGroups(groupIds).all().toCompletionStage())
                .thenApply(
                    g -> g.values().stream().sorted(comparing(ConsumerGroupDescription::groupId)))
                .toCompletableFuture()
                .join()
                .forEach(
                    g -> {
                      if (shortDisplay) {
                        printShort(g, System.out);
                      } else {
                        print(g, System.out);
                      }
                    }));
  }
}
