package net.pincette.jes.cli;

import picocli.CommandLine.Option;

class AggregateCommand {
  @Option(
      names = {"-a", "--aggregate"},
      required = true,
      description = "An aggregate type, which us usually constructed as <app>-<type>")
  String aggregate;

  @Option(
      names = {"-e", "--environment"},
      description =
          "The name of the environment, which is appended as a suffix to the MongoDB collection")
  String environment;

  @Option(
      names = {"-m", "--mongodb-url"},
      required = true,
      description = "The MongoDB connection URL")
  String mongoUrl;

  @Option(
      names = {"-mdb", "--mongodb-database"},
      required = true,
      description = "The MongoDB database")
  String mongoDatabase;
}
