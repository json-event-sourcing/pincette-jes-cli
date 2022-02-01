package net.pincette.jes.cli;

import static java.util.Comparator.comparing;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static net.pincette.jes.cli.Application.VERSION;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.composeAsyncStream;
import static net.pincette.util.Triple.triple;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import net.pincette.util.Pair;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
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
  private static CompletionStage<Long> getOffset(
      final TopicPartition partition, final Admin admin, final Supplier<OffsetSpec> spec) {
    return admin
        .listOffsets(map(pair(partition, spec.get())))
        .partitionResult(partition)
        .thenApply(ListOffsetsResultInfo::offset)
        .toCompletionStage();
  }

  private static String nodeList(final List<Node> nodes) {
    return nodes.stream().map(Node::host).collect(joining(","));
  }

  private Pair<Long, Long> getOffsets(final TopicPartitionInfo info, final Admin admin) {
    final TopicPartition partition = new TopicPartition(topic, info.partition());

    return getOffset(partition, admin, OffsetSpec::earliest)
        .thenComposeAsync(
            earliest ->
                getOffset(partition, admin, OffsetSpec::latest)
                    .thenApply(latest -> pair(earliest, latest)))
        .toCompletableFuture()
        .join();
  }

  private Collection<TopicPartitionInfo> getPartitions(final Admin admin) {
    return admin
        .describeTopics(set(topic))
        .all()
        .thenApply(
            t ->
                t.values().stream()
                    .flatMap(
                        v ->
                            v.partitions().stream()
                                .sorted(comparing(TopicPartitionInfo::partition)))
                    .collect(toList()))
        .toCompletionStage()
        .toCompletableFuture()
        .join();
  }

  private Collection<String> getProperties(final Admin admin) {
    return admin
        .describeConfigs(set(new ConfigResource(TOPIC, topic)))
        .all()
        .thenApply(
            result ->
                result.values().stream()
                    .flatMap(v -> v.entries().stream())
                    .map(e -> e.name() + "=" + e.value())
                    .sorted()
                    .collect(toList()))
        .toCompletionStage()
        .toCompletableFuture()
        .join();
  }

  private Map<Integer, Map<String, Long>> groupOffsets(
      final Collection<TopicPartitionInfo> partitions, final Admin admin) {
    return admin
        .listConsumerGroups()
        .valid()
        .thenApply(c -> c.stream().map(ConsumerGroupListing::groupId))
        .toCompletionStage()
        .thenComposeAsync(
            groups ->
                composeAsyncStream(
                    groups.map(
                        g ->
                            admin
                                .listConsumerGroupOffsets(g)
                                .partitionsToOffsetAndMetadata()
                                .thenApply(
                                    offsets ->
                                        offsets.entrySet().stream()
                                            .filter(e -> present(partitions, e.getKey()))
                                            .map(
                                                e ->
                                                    triple(
                                                        e.getKey().partition(),
                                                        g,
                                                        e.getValue().offset())))
                                .toCompletionStage())))
        .thenApply(
            triples ->
                triples
                    .flatMap(s -> s)
                    .collect(
                        groupingBy(
                            triple -> triple.first,
                            toMap(triple -> triple.second, triple -> triple.third))))
        .toCompletableFuture()
        .join();
  }

  private boolean present(
      final Collection<TopicPartitionInfo> partitions, final TopicPartition partition) {
    return partition.topic().equals(topic)
        && partitions.stream().anyMatch(p -> p.partition() == partition.partition());
  }

  @SuppressWarnings("java:S106") // Not logging.
  public void run() {
    runWithAdmin(
        admin -> {
          System.out.println("Properties:");
          getProperties(admin).forEach(p -> System.out.println("  " + p));
          System.out.println("Partitions:");

          final Collection<TopicPartitionInfo> partitions = getPartitions(admin);
          final Map<Integer, Map<String, Long>> groupOffsets = groupOffsets(partitions, admin);

          partitions.forEach(
              p -> {
                System.out.println("  " + p.partition() + ":");
                System.out.println("    ISRs: " + nodeList(p.isr()));
                System.out.println(
                    "    Leader: " + ofNullable(p.leader()).map(Node::host).orElse("none"));
                System.out.println("    Replicas: " + nodeList(p.replicas()));

                final Pair<Long, Long> offsets = getOffsets(p, admin);

                System.out.println("    Earliest offset: " + offsets.first);
                System.out.println("    Latest offset: " + offsets.second);
                System.out.println("    Group offsets:");

                ofNullable(groupOffsets.get(p.partition()))
                    .ifPresent(
                        o ->
                            o.entrySet().stream()
                                .sorted(Entry.comparingByKey())
                                .forEach(
                                    e ->
                                        System.out.println(
                                            "      " + e.getKey() + ": " + e.getValue())));
              });
        });
  }
}
