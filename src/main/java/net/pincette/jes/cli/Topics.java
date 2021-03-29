package net.pincette.jes.cli;

import static net.pincette.jes.cli.Application.VERSION;

import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;

@Command(
    name = "topics",
    version = VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {
      AlterTopic.class,
      ConsumeTopic.class,
      ConsumerGroups.class,
      CreateTopic.class,
      DeleteTopic.class,
      DescribeTopic.class,
      HelpCommand.class,
      ListTopics.class,
      MessageLag.class,
      ProduceTopic.class
    },
    description = "Commands to work with JSON Kafka topics.")
class Topics {}
