package net.pincette.jes.cli;

import static java.util.stream.Collectors.toMap;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Util.join;
import static net.pincette.util.Util.tryToDoRethrow;

import java.io.File;
import java.io.FileInputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import net.pincette.rs.LambdaSubscriber;
import org.reactivestreams.Publisher;

class Util {
  private Util() {}

  static Map<String, Object> fromProperties(final Properties properties) {
    return properties.entrySet().stream()
        .collect(toMap(e -> e.getKey().toString(), Entry::getValue));
  }

  static Properties loadProperties(final File file) {
    final Properties properties = new Properties();

    tryToDoRethrow(() -> properties.load(new FileInputStream(file)));

    return properties;
  }

  @SuppressWarnings("java:S106") // Not logging.
  static void print(final Publisher<String> publisher) {
    final Publisher<String> p = with(publisher).fanout().get();

    p.subscribe(
        new LambdaSubscriber<>(
            System.out::print,
            () -> {
              System.out.println();
              System.out.flush();
            }));

    join(p);
  }
}
