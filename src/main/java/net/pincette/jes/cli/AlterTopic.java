package net.pincette.jes.cli;

import static net.pincette.jes.cli.Application.VERSION;
import static net.pincette.jes.cli.Util.alterTopic;

import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;

@Command(
    name = "alter",
    version = VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description = "Changes the properties of a Kafka topic.")
class AlterTopic extends TopicCommand implements Runnable {
  @Option(
      names = {"-a", "--assignments"},
      required = true,
      description = "A comma-separated list of property assignments like \"p1=v1,p2=v2\".")
  private String assignments;

  public void run() {
    runWithAdmin(admin -> alterTopic(topic, assignments, admin));
  }
}
