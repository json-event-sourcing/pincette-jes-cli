package net.pincette.jes.cli;

import static net.pincette.jes.cli.Application.VERSION;

import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

@Command(
    name = "commands",
    version = VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class, SendCommand.class},
    description = "Commands to work with JSON Event Sourcing commands")
class Commands {}
