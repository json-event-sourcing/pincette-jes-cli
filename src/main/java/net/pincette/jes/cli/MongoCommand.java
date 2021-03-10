package net.pincette.jes.cli;

import picocli.CommandLine.Option;

class MongoCommand {
  @Option(
      names = {"-m", "--mongodb-url"},
      required = true,
      description = "The MongoDB connection URL.")
  String mongoUrl;

  @Option(
      names = {"-mdb", "--mongodb-database"},
      required = true,
      description = "The MongoDB database.")
  String mongoDatabase;
}
