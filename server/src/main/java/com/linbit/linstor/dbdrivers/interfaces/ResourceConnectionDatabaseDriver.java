package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link ResourceConnection}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface ResourceConnectionDatabaseDriver extends GenericDatabaseDriver<ResourceConnection>
{
    /**
     * A special sub-driver to update the persisted flags.
     */
    StateFlagsPersistence<ResourceConnection> getStateFlagPersistence();

    /**
     * A special sub-driver to update DRBD Proxy's TCP port for the source resource
     */
    SingleColumnDatabaseDriver<ResourceConnection, TcpPortNumber> getDrbdProxyPortSourceDriver();

    /**
     * A special sub-driver to update DRBD Proxy's TCP port for the target resource
     */
    SingleColumnDatabaseDriver<ResourceConnection, TcpPortNumber> getDrbdProxyPortTargetDriver();
}
