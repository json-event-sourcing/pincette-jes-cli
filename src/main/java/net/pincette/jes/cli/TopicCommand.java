package net.pincette.jes.cli;

import static java.lang.System.exit;
import static net.pincette.jes.cli.Util.loadProperties;
import static net.pincette.jes.cli.Util.topicExists;
import static net.pincette.util.Util.tryToDoWithRethrow;
import static org.apache.kafka.clients.admin.Admin.create;

import java.io.File;
import java.util.Properties;
import java.util.function.Consumer;
import net.pincette.function.ConsumerWithException;
import org.apache.kafka.clients.admin.Admin;
import picocli.CommandLine.Option;

class TopicCommand {
  @Option(
      names = {"-c", "--config-file"},
      required = true,
      description = "A Kafka configuration file.")
  File config;

  @Option(
      names = {"-t", "--topic"},
      required = true,
      description = "A Kafka topic.")
  String topic;

  protected void runWithAdmin(final ConsumerWithException<Admin> doIt) {
    runWithAdmin(doIt, true);
  }

  protected void runWithAdmin(final ConsumerWithException<Admin> doIt, final boolean checkTopic) {
    runWithConfig(properties -> tryToDoWithRethrow(() -> create(properties), doIt), checkTopic);
  }

  protected void runWithConfig(final Consumer<Properties> doIt) {
    runWithConfig(doIt, true);
  }

  @SuppressWarnings("java:S106") // Not logging.
  protected void runWithConfig(final Consumer<Properties> doIt, final boolean checkTopic) {
    final Properties properties = loadProperties(config);

    if (checkTopic && !topicExists(topic, properties)) {
      System.err.println("The Kafka topic " + topic + " doesn't exist.");
      exit(1);
    }

    doIt.accept(properties);
  }
}
