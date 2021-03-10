package net.pincette.jes.cli;

import static java.util.Optional.ofNullable;
import static net.pincette.jes.cli.Application.VERSION;
import static net.pincette.jes.cli.Util.loadProperties;
import static net.pincette.jes.prodcon.ConsoleConsumer.consume;

import java.io.File;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.mongo.Match;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;

@Command(
    name = "consume",
    version = VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description = "Consume JSON messages from a Kafka topic and write them to the terminal.")
class ConsumeTopic implements Runnable {
  @Option(
      names = {"-c", "--config-file"},
      required = true,
      description = "A ccloud configuration file.")
  private File config;

  @Option(
      names = {"-f", "--filter"},
      description = "A MongoDB expression to filter out objects.")
  private String filter;

  @Option(
      names = {"-t", "--topic"},
      required = true,
      description = "A Kafka topic.")
  private String topic;

  @SuppressWarnings("java:S106") // Not logging.
  public void run() {
    consume(
        loadProperties(config),
        topic,
        System.out,
        ofNullable(filter)
            .flatMap(JsonUtil::from)
            .filter(JsonUtil::isObject)
            .map(JsonValue::asJsonObject)
            .map(Match::predicate)
            .orElse(json -> true));
  }
}
