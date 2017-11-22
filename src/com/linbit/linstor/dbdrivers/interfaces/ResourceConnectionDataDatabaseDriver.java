package com.linbit.linstor.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.linbit.TransactionMgr;
import com.linbit.linstor.BaseTransactionObject;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceConnection;
import com.linbit.linstor.ResourceConnectionData;

/**
 * Database driver for {@link ResourceConnectionData}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface ResourceConnectionDataDatabaseDriver
{
    /**
     * Loads the {@link ResourceConnectionData} specified by the parameters
     * {@code source} and {@code target}.
     * <br>
     * By convention the {@link NodeName} of the node where {@code source} is assigned has to be
     * alphanumerically smaller than the {@link NodeName} of {@code target}' node.
     * @param source
     *  Part of the primary key specifying the database entry
     * @param target
     *  Part of the primary key specifying the database entry
     * @param logWarnIfNotExists
     *  If true a warning is logged if the requested entry does not exist
     * @param transMgr
     *  Used to restore references, like {@link com.linbit.linstor.Node}, {@link com.linbit.linstor.Resource}, and so on
     *
     * @return
     *  An instance which contains valid references, but is not
     *  initialized yet in regards of {@link BaseTransactionObject#initialized()}
     *
     * @throws SQLException
     */
    public ResourceConnectionData load(
        Resource source,
        Resource target,
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )
        throws SQLException;

    /**
     * Loads all {@link ResourceConnectionData} specified by the parameter
     * {@code resource}, regardless if the {@code resource} is the source of the target of the
     * specific {@link ResourceConnection}.
     * <br>
     * @param resource
     *  Part of the primary key specifying the database entry
     * @param transMgr
     *  Used to restore references, like {@link com.linbit.linstor.Node}, {@link com.linbit.linstor.Resource}, and so on
     *
     * @return
     *  A list of instances which contain valid references, but are not
     *  initialized yet in regards of {@link BaseTransactionObject#initialized()}
     *
     * @throws SQLException
     */
    public List<ResourceConnectionData> loadAllByResource(
        Resource resource,
        TransactionMgr transMgr
    )
        throws SQLException;


    /**
     * Persists the given {@link ResourceConnectionData} into the database.
     *
     * @param resConDfnData
     *  The data to be stored (including the primary key)
     * @param transMgr
     *  The {@link com.linbit.TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void create(ResourceConnectionData resConDfnData, TransactionMgr transMgr) throws SQLException;

    /**
     * Removes the given {@link ResourceConnectionData} from the database
     *
     * @param resConDfnData
     *  The data identifying the database entry to delete
     * @param transMgr
     *  The {@link com.linbit.TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void delete(ResourceConnectionData resConDfnData, TransactionMgr transMgr) throws SQLException;

}
