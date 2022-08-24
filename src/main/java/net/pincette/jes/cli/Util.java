package net.pincette.jes.cli;

import static java.lang.System.exit;
import static java.util.Arrays.stream;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static net.pincette.jes.JsonFields.ID;
import static net.pincette.jes.util.Kafka.createReliableProducer;
import static net.pincette.jes.util.Kafka.send;
import static net.pincette.json.JsonUtil.createReader;
import static net.pincette.rs.Chain.with;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.getStackTrace;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.tryToDoRethrow;
import static net.pincette.util.Util.tryToGetRethrow;
import static net.pincette.util.Util.tryToGetWithSilent;
import static org.apache.kafka.clients.admin.Admin.create;
import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;

import java.io.File;
import java.io.FileInputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Publisher;
import javax.json.JsonObject;
import net.pincette.json.JsonUtil;
import net.pincette.kafka.json.JsonSerializer;
import net.pincette.rs.LambdaSubscriber;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringSerializer;

class Util {
  private Util() {}

  static void alterTopic(final String topic, final String assignments, final Admin admin) {
    admin
        .incrementalAlterConfigs(map(pair(new ConfigResource(TOPIC, topic), ops(assignments))))
        .all()
        .toCompletionStage()
        .toCompletableFuture()
        .join();
  }

  static Set<String> commaSeparated(final String value) {
    return stream(value.split(",")).map(String::trim).collect(toSet());
  }

  static Map<String, Object> fromProperties(final Properties properties) {
    return properties.entrySet().stream()
        .collect(toMap(e -> e.getKey().toString(), Entry::getValue));
  }

  static Properties loadProperties(final File file) {
    final Properties properties = new Properties();

    tryToDoRethrow(() -> properties.load(new FileInputStream(file)));

    return properties;
  }

  private static Collection<AlterConfigOp> ops(final String assignments) {
    return stream(assignments.split(","))
        .map(a -> a.split("="))
        .filter(a -> a.length == 2)
        .map(a -> new ConfigEntry(a[0].trim(), a[1].trim()))
        .map(e -> new AlterConfigOp(e, SET))
        .collect(toList());
  }

  static KafkaProducer<String, JsonObject> producer(final File config) {
    return createReliableProducer(
        fromProperties(loadProperties(config)), new StringSerializer(), new JsonSerializer());
  }

  @SuppressWarnings("java:S106") // Not logging.
  static void print(final Publisher<String> publisher) {
    final CompletableFuture<Boolean> end = new CompletableFuture<>();

    publisher.subscribe(
        new LambdaSubscriber<>(
            System.out::print,
            () -> {
              System.out.println();
              System.out.flush();
              end.complete(true);
            },
            e -> {
              System.out.println(getStackTrace(e));
              exit(1);
            }));

    end.join();
  }

  static JsonObject readObject(final File file) {
    return tryToGetRethrow(() -> createReader(new FileInputStream(file)).readObject()).orElse(null);
  }

  static CompletionStage<Boolean> sendJson(
      final KafkaProducer<String, JsonObject> producer, final String topic, final JsonObject json) {
    return send(
            producer,
            new ProducerRecord<>(topic, json.getString(ID, randomUUID().toString()), json))
        .thenApply(result -> must(result, r -> r));
  }

  static Publisher<String> toArray(final Publisher<JsonObject> stream) {
    return with(stream).map(JsonUtil::string).separate(",").before("[").after("]").get();
  }

  static boolean topicExists(final String topic, final Properties config) {
    return tryToGetWithSilent(
            () -> create(config),
            admin ->
                admin
                    .describeTopics(list(topic))
                    .allTopicNames()
                    .thenApply(topics -> !topics.isEmpty())
                    .get())
        .orElse(false);
  }
}
