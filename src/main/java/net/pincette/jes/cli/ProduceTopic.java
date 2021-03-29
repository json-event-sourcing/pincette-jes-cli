package net.pincette.jes.cli;

import static java.util.logging.Level.SEVERE;
import static java.util.logging.Logger.getGlobal;
import static java.util.stream.Collectors.toMap;
import static javax.json.Json.createParser;
import static net.pincette.jes.cli.Application.VERSION;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.Kafka.createReliableProducer;
import static net.pincette.jes.util.Kafka.send;
import static net.pincette.util.StreamUtil.composeAsyncStream;
import static net.pincette.util.Util.getStackTrace;
import static net.pincette.util.Util.tryToGetWithRethrow;

import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import javax.json.JsonValue;
import net.pincette.function.SideEffect;
import net.pincette.jes.util.JsonSerializer;
import net.pincette.json.JsonUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

@Command(
    name = "produce",
    version = VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description = "Produce JSON messages to a Kafka topic taken from the terminal.")
class ProduceTopic extends TopicCommand implements Runnable {
  private static Map<String, Object> asMap(final Properties properties) {
    return properties.entrySet().stream()
        .collect(toMap(e -> e.getKey().toString(), Entry::getValue));
  }

  private static boolean logError(final Throwable e) {
    return SideEffect.<Boolean>run(() -> getGlobal().log(SEVERE, getStackTrace(e)))
        .andThenGet(() -> false);
  }

  private boolean produce(final Properties config, final InputStream in) {
    return tryToGetWithRethrow(
            () ->
                createReliableProducer(asMap(config), new StringSerializer(), new JsonSerializer()),
            producer ->
                composeAsyncStream(
                        net.pincette.json.filter.Util.stream(createParser(in))
                            .filter(JsonUtil::isObject)
                            .map(JsonValue::asJsonObject)
                            .filter(json -> json.containsKey(ID))
                            .map(
                                json ->
                                    send(
                                            producer,
                                            new ProducerRecord<>(topic, json.getString(ID), json))
                                        .exceptionally(ProduceTopic::logError)))
                    .thenApply(results -> results.reduce((r1, r2) -> r1 && r2).orElse(true))
                    .toCompletableFuture()
                    .get())
        .orElse(false);
  }

  public void run() {
    runWithConfig(properties -> produce(properties, System.in));
  }
}
