package net.pincette.jes.cli;

import static net.pincette.jes.cli.Application.VERSION;
import static net.pincette.jes.cli.Util.alterTopic;
import static net.pincette.util.Collections.set;

import java.util.Optional;
import org.apache.kafka.clients.admin.NewTopic;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;

@Command(
    name = "create",
    version = VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description = "Creates a new Kafka topic.")
class CreateTopic extends TopicCommand implements Runnable {
  @Option(
      names = {"-a", "--assignments"},
      description = "A comma-separated list of property assignments like \"p1=v1,p2=v2\".")
  private String assignments;

  @Option(
      names = {"-p", "--partitions"},
      description = "The number of partitions for the Kafka topic.")
  private int partitions = -1;

  @Option(
      names = {"-r", "--replication-factor"},
      description = "The replication factor for the Kafka topic.")
  private short replicationFactor = -1;

  public void run() {
    runWithAdmin(
        admin -> {
          admin
              .createTopics(
                  set(
                      new NewTopic(
                          topic,
                          Optional.of(partitions).filter(p -> p != -1),
                          Optional.of(replicationFactor).filter(r -> r != -1))))
              .all()
              .toCompletionStage()
              .toCompletableFuture()
              .join();

          if (assignments != null) {
            alterTopic(topic, assignments, admin);
          }
        },
        false);
  }
}
