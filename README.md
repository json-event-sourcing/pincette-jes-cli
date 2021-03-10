# JSON Event Sourcing Command Line Interface

This is a command line interface to do operational things. Currently there are the commands ```commands```, ```events```, ```mongo``` and ```topics```, each with a couple of subcommands. With the ```help``` command you can see the documentation.

You can build the tool with ```mvn clean package```. This will produce a self-contained JAR-file in the ```target``` directory with the form ```pincette-jes-cli-<version>-jar-with-dependencies.jar```. You can launch this JAR like this:

```
> java -jar pincette-jes-cli-<version>-jar-with-dependencies.jar help
```