package net.pincette.jes.cli;

import static net.pincette.jes.cli.Application.VERSION;
import static net.pincette.util.Util.initLogging;

import java.util.Optional;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

@Command(
    mixinStandardHelpOptions = true,
    version = VERSION,
    subcommands = {
      Commands.class,
      ConsumerGroups.class,
      Events.class,
      HelpCommand.class,
      Mongo.class,
      Topics.class
    },
    description = "The command-line for JSON Event Sourcing.")
public class Application {
  static final String VERSION = "3.1.1";

  private Application() {}

  public static void main(final String[] args) {
    initLogging();
    Optional.of(new CommandLine(new Application()).execute(args))
        .filter(code -> code != 0)
        .ifPresent(System::exit);
  }
}
