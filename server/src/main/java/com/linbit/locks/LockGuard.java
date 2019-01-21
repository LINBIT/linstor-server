package com.linbit.locks;

import java.util.concurrent.locks.Lock;

public class LockGuard implements AutoCloseable
{
    private static final boolean EXC_UNLOCK_CURRENT = true;

    private boolean acquired = false;

    private final Lock[] lockBundle;

    /**
     * Constructs a new LockGuard instance
     *
     * If {@code deferred} is {@code true}, then the specified locks are not acquired until the
     * {@code lock()} method is called; otherwise, the locks are acquired immediately.
     *
     * If an exception is encountered while attempting to acquire a lock, the locks are released again,
     * the exception is thrown and construction of the LockGuard instance fails.
     *
     * It is safe to use a LockGuard instance that has been initialized with {@code deferred = true}
     * as an {@code AutoCloseable} object. If {@code lock()} had not been called by the time the
     * {@code close()} method is called, then no attempt is made to release the locks.
     *
     * @param deferred control flag for the immediate or deferred acquisition of the specified locks
     * @param locksRef array of locks to be managed by the new LockGuard instance
     */
    LockGuard(final boolean deferred, final Lock... locksRef)
    {
        lockBundle = locksRef;
        if (!deferred)
        {
            lock();
        }
    }

    /**
     * Acquires the locks managed by the LockGuard instance
     *
     * If acquiring any of the locks fails due to an exception, the locks are released again
     * and the exception is thrown
     */
    public final void lock()
    {
        // Attempt to acquire all locks
        int idx = 0;
        try
        {
            while (idx < lockBundle.length)
            {
                lockBundle[idx].lock();
                ++idx;
            }
        }
        catch (RuntimeException exc)
        {
            // It is not specified by the interface specification of java.util.concurrent.locks.Lock
            // whether a call of Lock.locks() that threw an exception succeeded or failed to acquire
            // the lock, and while it would be reasonable to assume that it did not, it may be safer
            // to attempt to release the current lock nonetheless, even though it may not have been
            // locked in the first place.
            // This behavior can be controlled by configuring the constant EXC_UNLOCK_CURRENT
            if (!EXC_UNLOCK_CURRENT)
            {
                --idx;
            }
            // Unlock all locks that (may) have been acquired
            while (idx >= 0)
            {
                try
                {
                    lockBundle[idx].unlock();
                }
                catch (RuntimeException ignored)
                {
                }
                --idx;
            }
            // Rethrow the exception that caused the lock() call to fail
            throw exc;
        }
        acquired = true;
    }

    /**
     * Releases the locks managed by the LockGuard instance
     *
     * If the locks have not been acquired yet, this is a no-op
     */
    public final void unlock()
    {
        close();
    }

    /**
     * Releases the locks managed by the LockGuard instance
     *
     * If the locks have not been acquired yet, this is a no-op
     */
    @Override
    public final void close()
    {
        if (acquired)
        {
            RuntimeException savedExc = null;
            // Attempt to release all locks
            for (int idx = lockBundle.length - 1; idx >= 0; --idx)
            {
                try
                {
                    lockBundle[idx].unlock();
                }
                catch (RuntimeException rtExc)
                {
                    // Save the first exception
                    if (savedExc == null)
                    {
                        savedExc = rtExc;
                    }
                }
            }
            acquired = false;
            // Rethrow a saved exception to avoid hiding implementation errors
            if (savedExc != null)
            {
                throw savedExc;
            }
        }
    }

    /**
     * Constructs and returns a new LockGuard instance and acquires the specified locks immediately
     *
     * See the description of the LockGuard constructor for further details.
     *
     * @param locks array of locks to be managed by the new LockGuard instance
     * @return instance of the LockGuard class
     */
    public static final LockGuard createLocked(final Lock... locks)
    {
        return new LockGuard(false, locks);
    }

    /**
     * Constructs and returns a new LockGuard instance, but does not acquire the specified locks
     *
     * See the description of the LockGuard constructor for further details.
     *
     * @param locks array of locks to be managed by the new LockGuard instance
     * @return instance of the LockGuard class
     */
    public static final LockGuard createDeferred(final Lock... locks)
    {
        return new LockGuard(true, locks);
    }
}
