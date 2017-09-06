package com.linbit.drbdmanage.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.drbdmanage.Node.NodeFlag;
import com.linbit.drbdmanage.Node.NodeType;
import com.linbit.drbdmanage.NodeData;
import com.linbit.drbdmanage.dbdrivers.interfaces.BaseDatabaseDriver.BasePropsDatabaseDriver;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link NodeData}.
 *
 * @author Gabor Hernadi <gabor.hernadi@linbit.com>
 */
public interface NodeDataDatabaseDriver extends BasePropsDatabaseDriver<NodeData>
{
    /**
     * A special sub-driver to update the persisted {@link NodeFlag}s. The data record
     * is specified by the primary key stored as instance variables.
     *
     * @return
     */
    public StateFlagsPersistence getStateFlagPersistence();

    /**
     * A special sub-driver to update the persisted {@link NodeType}. The data record
     * is specified by the primary key stored as instance variables.
     *
     * @return
     */
    public SingleColumnDatabaseDriver<NodeType> getNodeTypeDriver();

    /**
     * A special sub-driver to update the instance specific {@link Props}. The data record
     * is specified by the primary key stored as instance variables.
     *
     * @return
     */
    public PropsConDatabaseDriver getPropsConDriver();

}
