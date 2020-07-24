package net.pincette.jes.cli;

import static net.pincette.jes.cli.Application.VERSION;

import java.util.Optional;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

@Command(
    mixinStandardHelpOptions = true,
    version = VERSION,
    subcommands = {Commands.class, Events.class, HelpCommand.class, Topics.class},
    description = "The command line for JSON Event Sourcing")
public class Application {
  static final String VERSION = "1.0";

  private Application() {}

  public static void main(final String[] args) {
    Optional.of(new CommandLine(new Application()).execute(args))
        .filter(code -> code != 0)
        .ifPresent(System::exit);
  }
}
