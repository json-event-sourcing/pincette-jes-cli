package net.pincette.jes.cli;

import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toMap;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.Kafka.createReliableProducer;
import static net.pincette.jes.util.Kafka.send;
import static net.pincette.rs.Chain.with;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.tryToDoRethrow;

import java.io.File;
import java.io.FileInputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.json.JsonObject;
import net.pincette.jes.util.JsonSerializer;
import net.pincette.json.JsonUtil;
import net.pincette.rs.LambdaSubscriber;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.reactivestreams.Publisher;

class Util {
  private Util() {}

  static Map<String, Object> fromProperties(final Properties properties) {
    return properties.entrySet().stream()
        .collect(toMap(e -> e.getKey().toString(), Entry::getValue));
  }

  static Properties loadProperties(final File file) {
    final Properties properties = new Properties();

    tryToDoRethrow(() -> properties.load(new FileInputStream(file)));

    return properties;
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
            }));

    end.join();
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
}
