package net.pincette.jes.cli;

import picocli.CommandLine.Option;

class AggregateCommand extends MongoCommand {
  @Option(
      names = {"-a", "--aggregate"},
      required = true,
      description = "An aggregate type, which us usually constructed as <app>-<type>.")
  String aggregate;

  @Option(
      names = {"-e", "--environment"},
      description =
          "The name of the environment, which is appended as a suffix to the MongoDB collection.")
  String environment;
}
