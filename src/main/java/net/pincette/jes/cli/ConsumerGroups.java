package net.pincette.jes.cli;

import static net.pincette.jes.cli.Application.VERSION;

import picocli.CommandLine.Command;

@Command(
    name = "consumer-groups",
    version = VERSION,
    mixinStandardHelpOptions = true,
    subcommands = {DeleteConsumerGroups.class, ListConsumerGroups.class, MessageLag.class},
    description = "Commands to work with Kafka consumer groups.")
class ConsumerGroups {}
