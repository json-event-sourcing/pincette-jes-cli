package net.pincette.jes.cli;

import static java.util.Comparator.comparing;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static net.pincette.jes.cli.Application.VERSION;
import static net.pincette.jes.util.Kafka.wrap;
import static net.pincette.util.Collections.set;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;

import java.util.Collection;
import java.util.List;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

@Command(
    name = "describe",
    version = VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description = "Describes a Kafka topic.")
class DescribeTopic extends TopicCommand implements Runnable {
  private static String nodeList(final List<Node> nodes) {
    return nodes.stream().map(Node::host).collect(joining(","));
  }

  private Collection<TopicPartitionInfo> getPartitions(final Admin admin) {
    return wrap(admin.describeTopics(set(topic)).all())
        .thenApply(
            t ->
                t.values().stream()
                    .flatMap(
                        v ->
                            v.partitions().stream()
                                .sorted(comparing(TopicPartitionInfo::partition)))
                    .collect(toList()))
        .toCompletableFuture()
        .join();
  }

  private Collection<String> getProperties(final Admin admin) {
    return wrap(admin.describeConfigs(set(new ConfigResource(TOPIC, topic))).all())
        .thenApply(
            result ->
                result.values().stream()
                    .flatMap(v -> v.entries().stream())
                    .map(e -> e.name() + "=" + e.value())
                    .sorted()
                    .collect(toList()))
        .toCompletableFuture()
        .join();
  }

  @SuppressWarnings("java:S106") // Not logging.
  public void run() {
    runWithAdmin(
        admin -> {
          System.out.println("Properties:");
          getProperties(admin).forEach(p -> System.out.println("  " + p));
          System.out.println("Partitions:");
          getPartitions(admin)
              .forEach(
                  p -> {
                    System.out.println("  " + p.partition() + ":");
                    System.out.println("    ISRs: " + nodeList(p.isr()));
                    System.out.println(
                        "    Leader: " + ofNullable(p.leader()).map(Node::host).orElse("none"));
                    System.out.println("    Replicas: " + nodeList(p.replicas()));
                  });
        });
  }
}
