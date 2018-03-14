package com.linbit.linstor.dbdrivers.interfaces;

import java.sql.SQLException;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Node.NodeFlag;
import com.linbit.linstor.Node.NodeType;
import com.linbit.linstor.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link NodeData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface NodeDataDatabaseDriver
{
    /**
     * Loads the {@link NodeData} specified by the parameter {@code nodeName}
     *
     * @param nodeName
     *  The primary key identifying the row to load
     * @param logWarnIfNotExists
     *  If true a warning is logged if the requested entry does not exist
     *
     * @throws SQLException
     */
    NodeData load(NodeName nodeName, boolean logWarnIfNotExists) throws SQLException;

    /**
     * Persists the given {@link NodeData} into the database.
     *
     * @param nodeData
     *  The data to be stored (including the primary key)
     * @throws SQLException
     */
    void create(NodeData nodeData) throws SQLException;

    /**
     * Removes the given {@link NodeData} from the database
     *
     * @param node
     *  The data identifying the database entry to delete
     * @throws SQLException
     */
    void delete(NodeData node) throws SQLException;

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
