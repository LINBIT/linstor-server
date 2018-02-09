package com.linbit.linstor;

import java.util.UUID;

/**
 * Defines a network path between two DRBD resources
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface NetworkPath extends DbgInstanceUuid
{
    UUID getUuid();
}
