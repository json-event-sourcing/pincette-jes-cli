package net.pincette.jes.cli;

import static net.pincette.jes.cli.Application.VERSION;

import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

@Command(
    name = "mongo",
    version = VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class, QueryCollection.class, StreamMongo.class},
    description = "Commands to work with MongoDB collections.")
class Mongo {}
