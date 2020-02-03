package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Node.Flags;
import com.linbit.linstor.core.objects.Node.Type;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link Node}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface NodeDatabaseDriver extends GenericDatabaseDriver<Node>
{
    /**
     * A special sub-driver to update the persisted {@link Flags}s. The data record
     * is specified by the primary key stored as instance variables.
     */
    StateFlagsPersistence<Node> getStateFlagPersistence();

    /**
     * A special sub-driver to update the persisted {@link Type}. The data record
     * is specified by the primary key stored as instance variables.
     */
    SingleColumnDatabaseDriver<Node, Node.Type> getNodeTypeDriver();
}
