package net.pincette.jes.cli;

import static net.pincette.jes.cli.Application.VERSION;

import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

@Command(
    name = "events",
    version = VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {
      GenerateEvent.class,
      GetEvents.class,
      HelpCommand.class,
      Reconstruct.class
    },
    description = "Commands to work with JSON Streams event logs.")
class Events {}
