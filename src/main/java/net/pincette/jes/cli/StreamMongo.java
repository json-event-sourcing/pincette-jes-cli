package net.pincette.jes.cli;

import static net.pincette.jes.cli.Application.VERSION;
import static net.pincette.jes.cli.Util.producer;
import static net.pincette.jes.cli.Util.sendJson;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Util.join;
import static net.pincette.util.Util.tryToDoWithRethrow;

import java.io.File;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;

@Command(
    name = "stream",
    version = VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description = "Streams a MongoDB aggregation result into a Kafka topic.")
class StreamMongo extends MongoAggregation implements Runnable {
  @Option(
      names = {"-c", "--config-file"},
      required = true,
      description = "A ccloud configuration file.")
  private File config;

  @Option(
      names = {"-t", "--topic"},
      required = true,
      description = "The Kafka topic the MongoDB result will be streamed to.")
  private String topic;

  public void run() {
    tryToDoWithRethrow(
        () -> producer(config),
        producer ->
            join(with(aggregate()).mapAsync(json -> sendJson(producer, topic, json)).get()));
  }
}
