package net.pincette.jes.cli;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.ofSeconds;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static net.pincette.jes.cli.Application.VERSION;
import static net.pincette.json.JsonUtil.createReader;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Or.tryWith;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.stream;
import static net.pincette.util.Util.doForever;
import static net.pincette.util.Util.tryToDoWithRethrow;
import static net.pincette.util.Util.tryToGetRethrow;

import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Predicate;
import javax.json.JsonObject;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import net.pincette.jes.util.JsonDeserializer;
import net.pincette.json.JsonUtil;
import net.pincette.mongo.Match;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;

@Command(
    name = "consume",
    version = VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description = "Consume JSON messages from a Kafka topic and write them to the terminal.")
class ConsumeTopic extends TopicCommand implements Runnable {
  @Option(
      names = {"-f", "--filter"},
      description =
          "A MongoDB expression to filter out objects. It can be a file or a JSON "
              + "string without spaces.")
  private String filter;

  @ArgGroup() private FromWhere fromWhere;

  @Option(
      names = {"-g", "--group-id"},
      description =
          "The Kafka consumer group. Note that this may interfere with a service that "
              + "uses the same group.")
  private String groupId;

  private static <K, V> Map<TopicPartition, Long> offsets(
      final KafkaConsumer<K, V> consumer,
      final Collection<TopicPartition> partitions,
      final Instant timestamp) {
    return map(
        consumer
            .offsetsForTimes(map(partitions.stream().map(p -> pair(p, timestamp.toEpochMilli()))))
            .entrySet()
            .stream()
            .map(e -> pair(e.getKey(), e.getValue().offset())));
  }

  private static <K, V> Collection<TopicPartition> partitions(
      final KafkaConsumer<K, V> consumer, final String topic) {
    while (consumer.assignment().isEmpty()) {
      consumer.poll(ofSeconds(1));
    }

    return consumer.assignment().stream().filter(p -> p.topic().equals(topic)).collect(toList());
  }

  private static void print(
      final ConsumerRecord<String, JsonObject> rec, final PrintWriter writer) {
    writer.println(rec.value());
    writer.flush();
  }

  private boolean consume(
      final Properties config, final OutputStream out, final Predicate<JsonObject> filter) {
    final PrintWriter writer = new PrintWriter(new OutputStreamWriter(out, UTF_8));

    config.setProperty("group.id", groupId != null ? groupId : randomUUID().toString());

    tryToDoWithRethrow(
        () -> new KafkaConsumer<>(config, new StringDeserializer(), new JsonDeserializer()),
        consumer -> {
          consumer.subscribe(set(topic));
          seek(
              consumer,
              topic,
              () ->
                  doForever(
                      () ->
                          stream(consumer.poll(ofSeconds(1)).iterator())
                              .filter(rec -> filter.test(rec.value()))
                              .forEach(rec -> print(rec, writer))));
        });

    return true;
  }

  private Optional<JsonStructure> readFilter() {
    return tryWith(
            () ->
                ofNullable(filter)
                    .filter(f -> new File(f).exists())
                    .map(
                        f ->
                            createReader(tryToGetRethrow(() -> new FileInputStream(f)).orElse(null))
                                .read())
                    .orElse(null))
        .or(() -> ofNullable(filter).flatMap(JsonUtil::from).orElse(null))
        .get();
  }

  @SuppressWarnings("java:S106") // Not logging.
  public void run() {
    runWithConfig(
        properties ->
            consume(
                properties,
                System.out,
                readFilter()
                    .filter(JsonUtil::isObject)
                    .map(JsonValue::asJsonObject)
                    .map(Match::predicate)
                    .orElse(json -> true)));
  }

  private <K, V> void seek(
      final KafkaConsumer<K, V> consumer, final String topic, final Runnable consume) {
    if (fromWhere != null) {
      if (fromWhere.beginning) {
        consumer.seekToBeginning(partitions(consumer, topic));
      } else {
        offsets(consumer, partitions(consumer, topic), fromWhere.timestamp).forEach(consumer::seek);
      }
    }

    consume.run();
  }

  private static class FromWhere {
    @Option(
        names = {"-b", "--beginning"},
        description = "Start consuming from the beginning of the topic.")
    private boolean beginning;

    @Option(
        names = {"-ts", "--timestamp"},
        description = "The UTC timestamp to start consuming from.")
    private Instant timestamp;
  }
}
