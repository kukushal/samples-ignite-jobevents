package samples.ignite.jobevents;

import org.apache.ignite.*;

public class App {
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("ignite-config.xml")) {
            int res = ignite.compute().execute(RandomSleepTask.class, 10);

            System.out.format("Total duration: %.2f sec.\n", res / 1000.0);
        }
    }
}
