package com.linbit.linstor.layer.storage.utils;

import com.linbit.ImplementationError;
import com.linbit.linstor.storage.StorageException;

import java.util.HashMap;
import java.util.Map;

/**
 * Counts concurrent opens of a device state that is shared between several openers, e.g. an activated snapshot LV or
 * the {@code snapdev} property of a zvol that affects all of its snapshots. Only the first open establishes the
 * state and only the last close reverts it, so one opener cannot pull the device away from under another.
 * <p>
 * The first open can return a state (e.g. whether the LV was inactive before) that is handed to the last close.
 * Opens and closes of all keys are serialized.
 *
 * @param <K> identifies the shared device state
 * @param <S> what the first open remembers for the last close
 */
public class SharedOpenCounter<K, S>
{
    /**
     * Establishes the shared device state for the first opener of a key.
     */
    @FunctionalInterface
    public interface FirstOpen<S>
    {
        /**
         * Returns what the last close needs to revert the device state.
         */
        S open() throws StorageException;
    }

    /**
     * Reverts the shared device state when the last opener of a key closes.
     */
    @FunctionalInterface
    public interface LastClose<S>
    {
        /**
         * Reverts the device state.
         *
         * @param state what the first open returned
         */
        void close(S state) throws StorageException;
    }

    private final Map<K, Entry<S>> entries = new HashMap<>();

    /**
     * Runs {@code firstOpen} if the given key is not open yet, then counts this open. If {@code firstOpen} fails,
     * nothing is counted.
     */
    public synchronized void open(K key, FirstOpen<S> firstOpen) throws StorageException
    {
        Entry<S> entry = entries.get(key);
        if (entry == null)
        {
            entries.put(key, new Entry<>(firstOpen.open()));
        }
        else
        {
            entry.count++;
        }
    }

    /**
     * Counts this close and runs {@code lastClose} with the state of the first open if it was the last open of the
     * given key. The key is closed even if {@code lastClose} fails.
     *
     * @throws ImplementationError if the key is not open
     */
    public synchronized void close(K key, LastClose<S> lastClose) throws StorageException
    {
        Entry<S> entry = entries.get(key);
        if (entry == null)
        {
            throw new ImplementationError("Closing " + key + ", which is not open");
        }
        entry.count--;
        if (entry.count == 0)
        {
            entries.remove(key);
            lastClose.close(entry.state);
        }
    }

    private static class Entry<S>
    {
        private final S state;
        private int count = 1;

        Entry(S stateRef)
        {
            state = stateRef;
        }
    }
}
