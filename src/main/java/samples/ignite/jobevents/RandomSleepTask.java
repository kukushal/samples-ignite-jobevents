package samples.ignite.jobevents;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

@ComputeTaskSessionFullSupport
public class RandomSleepTask extends ComputeTaskSplitAdapter<Integer, Integer> {
    @TaskSessionResource private ComputeTaskSession ses;
    private static final Random random = new Random();

    @Override protected Collection<? extends ComputeJob> split(int gridSize, Integer total) throws IgniteException {
        final AtomicInteger cntr = new AtomicInteger(0);
        final AtomicInteger duration = new AtomicInteger(0);

        ses.addAttributeListener((key, val) -> {
            if ("COMPLETE".compareTo(key.toString()) == 0) {
                int newCntr = cntr.incrementAndGet();
                int newDuration = duration.addAndGet((int)val);

                // Predict remaining duration assuming remaining jobs will have duration equal to the average of the
                // completed ones
                int avgDuration = newDuration / newCntr;
                int pendingTime = (total - newCntr) * avgDuration;

                System.out.format(
                    "%s\tout of %s\ttasks complete, %.2f\tmore seconds remaining\n",
                    newCntr,
                    total,
                    pendingTime / 1000.0);
            }
        }, false);

        return IntStream.range(0, total).mapToObj(i -> new ComputeJobAdapter() {
            @Override public Object execute() throws IgniteException {
                int duration = 150 + random.nextInt(100);

                try {
                    Thread.sleep(duration);
                }
                catch (InterruptedException e) {
                    throw new IgniteException(e);
                }

                ses.setAttribute("COMPLETE", duration);

                return duration;
            }
        }).collect(Collectors.toList());
    }

    @Nullable @Override public Integer reduce(List<ComputeJobResult> results) throws IgniteException {
        return results.stream().mapToInt(ComputeJobResult::getData).sum();
    }
}
