package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.objects.Node.NodeFlag;
import com.linbit.linstor.core.objects.Node.NodeType;
import com.linbit.linstor.core.objects.NodeData;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link NodeData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface NodeDataDatabaseDriver extends GenericDatabaseDriver<NodeData>
{
    /**
     * A special sub-driver to update the persisted {@link NodeFlag}s. The data record
     * is specified by the primary key stored as instance variables.
     */
    StateFlagsPersistence<NodeData> getStateFlagPersistence();

    /**
     * A special sub-driver to update the persisted {@link NodeType}. The data record
     * is specified by the primary key stored as instance variables.
     */
    SingleColumnDatabaseDriver<NodeData, NodeType> getNodeTypeDriver();
}
