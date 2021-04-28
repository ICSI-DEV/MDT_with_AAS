package plantpulse.cep.engine.pipeline.dataflow.strategy;


import java.util.concurrent.atomic.AtomicInteger;


/**
 * RoundRobinStrategy
 * @author leesa
 *
 */
public class RoundRobinStrategy  {

    private final int totalIndexes;
    private final AtomicInteger atomicInteger = new AtomicInteger(-1);

    public RoundRobinStrategy(int totalIndexes) {
        this.totalIndexes = totalIndexes;
    }

    public int index() {
        int currentIndex;
        int nextIndex;

        do {
            currentIndex = atomicInteger.get();
            nextIndex = currentIndex< Integer.MAX_VALUE ? currentIndex+1: 0;
        } while (!atomicInteger.compareAndSet(currentIndex, nextIndex));

        return nextIndex % totalIndexes;
    }

}
