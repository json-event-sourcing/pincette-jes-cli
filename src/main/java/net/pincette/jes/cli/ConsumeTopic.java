package net.pincette.jes.cli;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Duration.ofSeconds;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static net.pincette.jes.cli.Application.VERSION;
import static net.pincette.util.Collections.set;
import static net.pincette.util.StreamUtil.stream;
import static net.pincette.util.Util.doForever;
import static net.pincette.util.Util.tryToDoWithRethrow;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Properties;
import java.util.function.Predicate;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.jes.util.JsonDeserializer;
import net.pincette.json.JsonUtil;
import net.pincette.mongo.Match;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
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
      description = "A MongoDB expression to filter out objects.")
  private String filter;

  @Option(
      names = {"-g", "--group-id"},
      description =
          "The Kafka consumer group. Note that this may interfere with a service that "
              + "uses the same group.")
  private String groupId;

  private static void print(
      final ConsumerRecord<String, JsonObject> record, final PrintWriter writer) {
    writer.println(record.value());
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

          doForever(
              () ->
                  stream(consumer.poll(ofSeconds(1)).iterator())
                      .filter(record -> filter.test(record.value()))
                      .forEach(record -> print(record, writer)));
        });

    return true;
  }

  @SuppressWarnings("java:S106") // Not logging.
  public void run() {
    runWithConfig(
        properties ->
            consume(
                properties,
                System.out,
                ofNullable(filter)
                    .flatMap(JsonUtil::from)
                    .filter(JsonUtil::isObject)
                    .map(JsonValue::asJsonObject)
                    .map(Match::predicate)
                    .orElse(json -> true)));
  }
}
