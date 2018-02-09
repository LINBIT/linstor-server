package com.linbit.linstor.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.linbit.TransactionMgr;
import com.linbit.linstor.BaseTransactionObject;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeConnectionData;
import com.linbit.linstor.NodeName;

/**
 * Database driver for {@link NodeConnectionData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface NodeConnectionDataDatabaseDriver
{
    /**
     * Loads the {@link NodeConnectionData} specified by the parameters
     * {@code sourceNode} and {@code targetNode}.
     * <br>
     * By convention the {@link NodeName} of {@code sourceNode} has to be alphanumerically
     * smaller than the {@link NodeName} of {@code targetNode}
     * @param sourceNode
     *  Part of the primary key specifying the database entry
     * @param targetNode
     *  Part of the primary key specifying the database entry
     * @param logWarnIfNotExists
     *  If true a warning is logged if the requested entry does not exist
     * @param transMgr
     *  Used to restore references, like {@link com.linbit.linstor.Node}, {@link com.linbit.linstor.Resource},
     *  etc.
     *
     * @return
     *  An instance which contains valid references, but is not
     *  initialized yet in regards of {@link BaseTransactionObject#initialized()}
     *
     * @throws SQLException
     */
    NodeConnectionData load(
        Node sourceNode,
        Node targetNode,
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )
        throws SQLException;

    /**
     * Loads all {@link com.linbit.linstor.NodeConnectionData} specified by the parameter
     * {@code node}, regardless if {@code node} is the source or the target of the
     * specific {@link com.linbit.linstor.NodeConnection}.
     * <br>
     * @param transMgr
     *  Used to restore references, like {@link com.linbit.linstor.Node}, {@link com.linbit.linstor.Resource},
     *  etc.
     *
     * @return
     *  A list of instances which contain valid references, but are not
     *  initialized yet in regards of {@link com.linbit.linstor.BaseTransactionObject#initialized()}
     *
     * @throws SQLException
     */
    List<NodeConnectionData> loadAllByNode(
        Node node,
        TransactionMgr transMgr
    )
        throws SQLException;

    /**
     * Persists the given {@link com.linbit.linstor.NodeConnectionData} into the database.
     *
     * @param nodeConDfnData
     *  The data to be stored (including the primary key)
     * @param transMgr
     *  The {@link com.linbit.TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    void create(NodeConnectionData nodeConDfnData, TransactionMgr transMgr) throws SQLException;

    /**
     * Removes the given {@link NodeConnectionData} from the database
     *
     * @param nodeConDfnData
     *  The data identifying the database entry to delete
     * @param transMgr
     *  The {@link com.linbit.TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    void delete(NodeConnectionData nodeConDfnData, TransactionMgr transMgr) throws SQLException;

}
