package plantpulse.cep.engine.async.throttling;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class RateLimiter {

    public static void main(String[] args) {
        ExecutorService throttlingExecutorService = ThrottlingExecutorService.createExecutorService(200, 200, TimeUnit.SECONDS);
        for (Integer i = 0; i < 2000; i++) {
            throttlingExecutorService.execute(new PrintTask(i));
        }
        throttlingExecutorService.shutdown();
    }
}
