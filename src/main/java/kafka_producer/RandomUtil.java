package kafka_producer;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class RandomUtil {

    private RandomUtil() {
        throw new IllegalStateException("Class kafka_producer.RandomUtil is an utility class !");
    }

    public static UUID randomUUID() {
        return new UUID(ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextLong());
    }
}