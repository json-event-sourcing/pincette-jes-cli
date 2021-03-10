package net.pincette.jes.cli;

import static net.pincette.jes.cli.Application.VERSION;
import static net.pincette.jes.cli.Util.print;
import static net.pincette.jes.cli.Util.toArray;

import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

@Command(
    name = "query",
    version = VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description = "Queries a MongoDB collection and writes the resulting JSON array to stdout.")
class QueryCollection extends MongoAggregation implements Runnable {
  public void run() {
    print(toArray(aggregate()));
  }
}
