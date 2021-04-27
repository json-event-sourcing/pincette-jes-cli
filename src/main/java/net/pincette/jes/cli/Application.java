package net.pincette.jes.cli;

import static java.lang.ClassLoader.getSystemResourceAsStream;
import static java.lang.System.getProperty;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.LogManager.getLogManager;
import static net.pincette.jes.cli.Application.VERSION;
import static net.pincette.util.Util.tryToDoRethrow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Level;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

@Command(
    mixinStandardHelpOptions = true,
    version = VERSION,
    subcommands = {Commands.class, Events.class, HelpCommand.class, Mongo.class, Topics.class},
    description = "The command line for JSON Event Sourcing")
public class Application {
  static final String VERSION = "1.2.1";

  private Application() {}

  private static void initLogging(final Level level) {
    if (getProperty("java.util.logging.config.class") == null
        && getProperty("java.util.logging.config.file") == null) {
      final Properties logging = loadLogging();
      final ByteArrayOutputStream out = new ByteArrayOutputStream();

      logging.setProperty(".level", level.getName());

      tryToDoRethrow(
          () -> {
            logging.store(out, null);
            getLogManager().readConfiguration(new ByteArrayInputStream(out.toByteArray()));
          });
    }
  }

  private static Properties loadLogging() {
    final Properties properties = new Properties();

    tryToDoRethrow(() -> properties.load(getSystemResourceAsStream("logging.properties")));

    return properties;
  }

  public static void main(final String[] args) {
    initLogging(SEVERE);
    Optional.of(new CommandLine(new Application()).execute(args))
        .filter(code -> code != 0)
        .ifPresent(System::exit);
  }
}
