package com.linbit.linstor;

import com.linbit.linstor.annotation.Nullable;

import java.util.UUID;

/**
 * Enables identifying object instances by a UUID generated during object construction
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface DbgInstanceUuid
{
    /**
     * Returns a UUID that identifies an object. May be null if debugging is disabled or not compiled-in.
     *
     * @return UUID that identifies the instance
     */
    @Nullable
    UUID debugGetVolatileUuid();
}
