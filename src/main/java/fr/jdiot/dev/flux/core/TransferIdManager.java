package fr.jdiot.dev.flux.core;

import java.util.BitSet;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransferIdManager {

  private final int min;
  private final int max;
  private final int rangeSize;

  // BitSet is used to track used IDs.
  // False = Available, True = Taken.
  private final BitSet usedIds;

  // Lock for thread safety to prevent two transfers getting the same ID
  private final Object lock = new Object();

  // Tracks the last allocated index to provide a Round-Robin allocation
  private int lastIndex = 0;

  /**
   * Constructor to define the range of IDs.
   * 
   * @param min The minimum ID value (inclusive). Must be positive.
   * @param max The maximum ID value (inclusive).
   */
  public TransferIdManager(final int min, final int max) {
    if (min < 0) {
      throw new IllegalArgumentException("Min value must be positive.");
    }
    if (max < min) {
      throw new IllegalArgumentException("Max value must be greater than or equal to min.");
    }

    // Check for overflow when calculating range size
    final long size = (long) max - (long) min + 1L;
    if (size > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Range size exceeds Integer.MAX_VALUE.");
    }

    this.min = min;
    this.max = max;
    this.rangeSize = (int) size;
    this.usedIds = new BitSet(this.rangeSize);
  }

  /**
   * Acquires a unique ID from the pool. If an ID was released, it may be reused.
   * Uses a round-robin strategy.
   * 
   * @return A unique integer ID.
   * @throws IllegalStateException if all IDs in the range are currently in use.
   */
  public int acquireId() {
    synchronized (this.lock) {
      // Find the index of the first bit that is set to false (0) starting from lastIndex
      int nextClearBit = this.usedIds.nextClearBit(this.lastIndex);

      // If we've reached the end of the range, wrap around to the beginning
      if (nextClearBit >= this.rangeSize) {
        nextClearBit = this.usedIds.nextClearBit(0);
      }

      // If the index is STILL outside our range size, it means all bits are set (full)
      if (nextClearBit >= this.rangeSize) {
        throw new IllegalStateException(
            "No more transfer IDs available. All IDs from " + this.min + " to " + this.max + " are in use.");
      }

      // Mark this ID as used
      this.usedIds.set(nextClearBit);

      // Update lastIndex for the next acquireId call (wrap around if needed)
      this.lastIndex = nextClearBit + 1;
      if (this.lastIndex >= this.rangeSize) {
        this.lastIndex = 0;
      }

      // Convert the index back to the actual ID value
      final int newId = this.min + nextClearBit;
      TransferIdManager.log.debug("Generate id {}", newId);
      return newId;
    }
  }

  /**
   * Releases an ID back to the pool so it can be reused.
   * 
   * @param id The ID to release.
   * @throws IllegalArgumentException if the ID is out of bounds or was not
   *                                  currently active.
   */
  public void releaseId(final int id) {
    synchronized (this.lock) {
      // Validate bounds
      if (id < this.min || id > this.max) {
        throw new IllegalArgumentException("ID " + id + " is out of the generator's range (" + this.min + "-" + this.max + ")");
      }

      final int index = id - this.min;

      // Check if it was actually in use
      if (!this.usedIds.get(index)) {
        throw new IllegalArgumentException("ID " + id + " is not currently in use, cannot release it.");
      }

      TransferIdManager.log.debug("Release id {}", id);

      // Mark as available (false)
      this.usedIds.clear(index);
    }
  }

  /**
   * Utility to check how many IDs are currently taken.
   */
  public int getUsedCount() {
    synchronized (this.lock) {
      return this.usedIds.cardinality();
    }
  }
}
