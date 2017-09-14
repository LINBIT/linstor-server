package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.BaseTransactionObject;
import com.linbit.drbdmanage.ConnectionDefinitionData;
import com.linbit.drbdmanage.NodeName;
import com.linbit.drbdmanage.ResourceDefinition;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.propscon.SerialPropsContainer;

/**
 * Database driver for {@link ConnectionDefinitionData}.
 *
 * @author Gabor Hernadi <gabor.hernadi@linbit.com>
 */
public interface ConnectionDefinitionDataDatabaseDriver
{
    /**
     * Loads the {@link ConnectionDefinitionData} specified by the parameters
     * {@code resourceName}, {@code sourceNodeName} and {@code targetNodeName}.
     * <br />
     * By convention the {@code sourceNodeName} has to be alphanumerically smaller than {@code targetNodeName}
     *
     * @param resourceName
     *  Part of the primary key specifying the database entry
     * @param sourceNodeName
     *  Part of the primary key specifying the database entry
     * @param targetNodeName
     *  Part of the primary key specifying the database entry
     * @param serialGen
     *  Used to initialize the {@link SerialPropsContainer}
     * @param transMgr
     *  Used to restore references, like {@link Node}, {@link Resource}, and so on
     * @return
     *  An instance which contains valid references, but is not
     *  initialized yet in regards of {@link BaseTransactionObject#initialized()}
     *
     * @throws SQLException
     */
    public ConnectionDefinitionData load(
        ResourceDefinition resDfn,
        NodeName sourceNodeName,
        NodeName targetNodeName,
        SerialGenerator serialGen,
        TransactionMgr transMgr
    )
        throws SQLException;

    /**
     * Persists the given {@link ConnectionDefinitionData} into the database.
     *
     * @param conDfnData
     *  The data to be stored (including the primary key)
     * @param transMgr
     *  The {@link TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void create(ConnectionDefinitionData conDfnData, TransactionMgr transMgr) throws SQLException;

    /**
     * Removes the given {@link ConnectionDefinitionData} from the database
     *
     * @param conDfnData
     *  The data identifying the database entry to delete
     * @param transMgr
     *  The {@link TransactionMgr} containing the used database {@link Connection}
     * @throws SQLException
     */
    public void delete(ConnectionDefinitionData conDfnData, TransactionMgr transMgr) throws SQLException;

    /**
     * A special sub-driver to update the persisted connectionNumber
     */
    public SingleColumnDatabaseDriver<ConnectionDefinitionData, Integer> getConnectionNumberDriver();

}
