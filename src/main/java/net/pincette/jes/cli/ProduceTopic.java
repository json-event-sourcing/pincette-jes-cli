package net.pincette.jes.cli;

import static net.pincette.jes.cli.Application.VERSION;
import static net.pincette.jes.cli.Util.loadProperties;
import static net.pincette.jes.prodcon.ConsoleProducer.produce;

import java.io.File;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;

@Command(
    name = "produce",
    version = VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description = "Produce JSON messages to a Kafka topic taken from the terminal")
class ProduceTopic implements Runnable {
  @Option(
      names = {"-c", "--config-file"},
      required = true,
      description = "A ccloud configuration file")
  private File config;

  @Option(
      names = {"-t", "--topic"},
      required = true,
      description = "A Kafka topic")
  private String topic;

  public void run() {
    produce(loadProperties(config), topic, System.in);
  }
}
