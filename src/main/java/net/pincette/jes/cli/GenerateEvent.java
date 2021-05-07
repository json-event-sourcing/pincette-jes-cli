package net.pincette.jes.cli;

import static java.lang.String.valueOf;
import static java.time.Instant.now;
import static java.util.Arrays.fill;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static net.pincette.jes.cli.Application.VERSION;
import static net.pincette.jes.cli.Util.readObject;
import static net.pincette.jes.util.Commands.PUT;
import static net.pincette.jes.util.JsonFields.COMMAND;
import static net.pincette.jes.util.JsonFields.CORR;
import static net.pincette.jes.util.JsonFields.ID;
import static net.pincette.jes.util.JsonFields.OPS;
import static net.pincette.jes.util.JsonFields.SEQ;
import static net.pincette.jes.util.JsonFields.TIMESTAMP;
import static net.pincette.jes.util.JsonFields.TYPE;
import static net.pincette.jes.util.Util.removeTechnical;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createDiff;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.string;

import java.io.File;
import javax.json.JsonArray;
import javax.json.JsonObject;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;

@Command(
    name = "generate",
    version = VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description =
        "Generate the event that expresses the changes between aggregate instance versions.")
class GenerateEvent implements Runnable {
  @Option(
      names = {"-f", "--from"},
      required = true,
      description = "The file with the aggregate instance version to compare with.")
  private File from;

  @Option(
      names = {"-t", "--to"},
      required = true,
      description = "The file with the aggregate instance version to compare.")
  private File to;

  private static JsonArray createOps(final JsonObject oldState, final JsonObject newState) {
    return createArrayBuilder(
            createDiff(removeTechnical(oldState).build(), removeTechnical(newState).build())
                .toJsonArray())
        .build();
  }

  private static String generateSeq(final long value) {
    return pad(valueOf(value), '0', 12);
  }

  private static String id(final JsonObject json) {
    return ofNullable(json.getString(ID, null))
        .map(String::toLowerCase)
        .orElseGet(() -> randomUUID().toString());
  }

  private static String pad(final String s, final char c, final int size) {
    return s.length() >= size ? s : (new String(pad(c, size - s.length())) + s);
  }

  private static char[] pad(final char c, final int size) {
    final char[] result = new char[size];

    fill(result, c);

    return result;
  }

  @SuppressWarnings("java:S106") // Not logging.
  public void run() {
    final JsonObject newState = readObject(to);
    final int seq = newState.getInt(SEQ, -1);

    System.out.println(
        string(
            createObjectBuilder()
                .add(CORR, randomUUID().toString())
                .add(ID, id(newState) + "-" + generateSeq(seq))
                .add(TYPE, newState.getString(TYPE, ""))
                .add(SEQ, seq)
                .add(COMMAND, PUT)
                .add(TIMESTAMP, now().toEpochMilli())
                .add(OPS, createOps(readObject(from), newState))
                .build(),
            true));
  }
}
