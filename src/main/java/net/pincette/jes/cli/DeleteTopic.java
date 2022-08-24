package net.pincette.jes.cli;

import static net.pincette.jes.cli.Application.VERSION;
import static net.pincette.util.Collections.set;

import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

@Command(
    name = "delete",
    version = VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {HelpCommand.class},
    description = "Deletes a Kafka topic.")
class DeleteTopic extends TopicCommand implements Runnable {
  public void run() {
    runWithAdmin(
        admin ->
            admin.deleteTopics(set(topic)).all().toCompletionStage().toCompletableFuture().join());
  }
}
