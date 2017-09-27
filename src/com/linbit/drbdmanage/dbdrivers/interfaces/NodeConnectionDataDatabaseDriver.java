package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.BaseTransactionObject;
import com.linbit.drbdmanage.Node;
import com.linbit.drbdmanage.NodeConnection;
import com.linbit.drbdmanage.NodeConnectionData;
import com.linbit.drbdmanage.NodeName;

/**
 * Database driver for {@link NodeConnectionData}.
 *
 * @author Gabor Hernadi <gabor.hernadi@linbit.com>
 */
public interface NodeConnectionDataDatabaseDriver
{
    /**
     * Loads the {@link NodeConnectionData} specified by the parameters
     * {@code sourceNode} and {@code targetNode}.
     * <br />
     * By convention the {@link NodeName} of {@code sourceNode} has to be alphanumerically
     * smaller than the {@link NodeName} of {@code targetNode}
     * @param sourceNode
     *  Part of the primary key specifying the database entry
     * @param targetNode
     *  Part of the primary key specifying the database entry
     * @param logWarnIfNoTExists
     *  If true a warning is logged if the requested entry does not exist
     * @param transMgr
     *  Used to restore references, like {@link Node}, {@link Resource}, and so on
     *
     * @return
     *  An instance which contains valid references, but is not
     *  initialized yet in regards of {@link BaseTransactionObject#initialized()}
     *
     * @throws SQLException
     */
    public NodeConnectionData load(
        Node sourceNode,
        Node targetNode,
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )
        throws SQLException;

    /**
     * Loads all {@link NodeConnectionData} specified by the parameter
     * {@code node}, regardless if {@code node} is the source or the target of the
     * specific {@link NodeConnection}.
     * <br />
     * By convention the {@link NodeName} of {@code sourceNode} has to be alphanumerically
     * smaller than the {@link NodeName} of {@code targetNode}
     * @param sourceNode
     *  Part of the primary key specifying the database entry
     * @param targetNode
     *  Part of the primary key specifying the database entry
     * @param transMgr
     *  Used to restore references, like {@link Node}, {@link Resource}, and so on
     *
     * @return
     *  A list of instances which contain valid references, but are not
     *  initialized yet in regards of {@link BaseTransactionObject#initialized()}
     *
     * @throws SQLException
     */
    public List<NodeConnectionData> loadAllByNode(
        Node node,
        TransactionMgr transMgr
    )
        throws SQLException;

    /**
     * Persists the given {@link NodeConnectionData} into the database.
     *
     * @param nodeConDfnData
     *  The data to be stored (including the primary key)
     * @param transMgr
     *  The {@link TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void create(NodeConnectionData nodeConDfnData, TransactionMgr transMgr) throws SQLException;

    /**
     * Removes the given {@link NodeConnectionData} from the database
     *
     * @param nodeConDfnData
     *  The data identifying the database entry to delete
     * @param transMgr
     *  The {@link TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void delete(NodeConnectionData nodeConDfnData, TransactionMgr transMgr) throws SQLException;

}
