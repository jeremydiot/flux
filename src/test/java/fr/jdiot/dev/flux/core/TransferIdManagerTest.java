package fr.jdiot.dev.flux.core;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TransferIdManagerTest {

  @Test
  void testValidInitialization() {
    final TransferIdManager manager = new TransferIdManager(10, 20);
    MatcherAssert.assertThat(manager.getUsedCount(), Matchers.is(0));
  }

  @Test
  void testInvalidInitialization() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new TransferIdManager(-1, 10));
    Assertions.assertThrows(IllegalArgumentException.class, () -> new TransferIdManager(10, 5));
    // Test for overflow
    Assertions.assertThrows(IllegalArgumentException.class, () -> new TransferIdManager(0, Integer.MAX_VALUE));
  }

  @Test
  void testSequentialAllocationAndRoundRobin() {
    final TransferIdManager manager = new TransferIdManager(10, 12); // size 3: 10, 11, 12

    final int id1 = manager.acquireId();
    final int id2 = manager.acquireId();
    final int id3 = manager.acquireId();

    MatcherAssert.assertThat(id1, Matchers.is(10));
    MatcherAssert.assertThat(id2, Matchers.is(11));
    MatcherAssert.assertThat(id3, Matchers.is(12));
    MatcherAssert.assertThat(manager.getUsedCount(), Matchers.is(3));

    // Should throw IllegalStateException since it's full
    Assertions.assertThrows(IllegalStateException.class, manager::acquireId);

    // Release id 10
    manager.releaseId(10);
    MatcherAssert.assertThat(manager.getUsedCount(), Matchers.is(2));

    // Acquire again, should get 10 due to wrap-around (since lastIndex was 3 ->
    // wrapped to 0)
    final int id4 = manager.acquireId();
    MatcherAssert.assertThat(id4, Matchers.is(10));

    // Test round robin precisely
    manager.releaseId(11);
    manager.releaseId(12);
    // lastIndex is 1 right now (from id 10 which was at index 0)
    MatcherAssert.assertThat(manager.acquireId(), Matchers.is(11));
    MatcherAssert.assertThat(manager.acquireId(), Matchers.is(12));
  }

  @Test
  void testReleaseInvalidId() {
    final TransferIdManager manager = new TransferIdManager(10, 15);

    // Out of bounds
    Assertions.assertThrows(IllegalArgumentException.class, () -> manager.releaseId(9));
    Assertions.assertThrows(IllegalArgumentException.class, () -> manager.releaseId(16));

    // Not in use
    Assertions.assertThrows(IllegalArgumentException.class, () -> manager.releaseId(10));

    // Acquire and release twice
    manager.acquireId(); // gets 10
    manager.releaseId(10);
    Assertions.assertThrows(IllegalArgumentException.class, () -> manager.releaseId(10));
  }

  @Test
  void testConcurrentAccess() throws InterruptedException {
    final int poolSize = 1000;
    final TransferIdManager manager = new TransferIdManager(1, poolSize);
    final int threadCount = 100;
    final int iterations = 1000;

    final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    final CountDownLatch latch = new CountDownLatch(threadCount);

    // To ensure no two threads ever hold the same ID at the same time
    final ConcurrentHashMap<Integer, Boolean> currentlyHeldIds = new ConcurrentHashMap<>();

    // Track any concurrency failures
    final AtomicInteger errors = new AtomicInteger(0);

    for (int i = 0; i < threadCount; i++) {
      executor.submit(() -> {
        try {
          for (int j = 0; j < iterations; j++) {
            final int id = manager.acquireId();

            // Check if another thread is currently holding this ID
            if (currentlyHeldIds.putIfAbsent(id, Boolean.TRUE) != null) {
              errors.incrementAndGet(); // ID was already held!
            }

            // Yield to encourage context switching
            Thread.yield();

            // Release the ID
            currentlyHeldIds.remove(id);
            manager.releaseId(id);
          }
        } catch (final Exception e) {
          e.printStackTrace();
          errors.incrementAndGet();
        } finally {
          latch.countDown();
        }
      });
    }

    latch.await(10, TimeUnit.SECONDS);
    executor.shutdownNow();

    MatcherAssert.assertThat("There should be zero concurrency errors", errors.get(), Matchers.is(0));
    MatcherAssert.assertThat("All IDs should be released", manager.getUsedCount(), Matchers.is(0));
    MatcherAssert.assertThat("Map should be empty", currentlyHeldIds.isEmpty(), Matchers.is(true));
  }
}
