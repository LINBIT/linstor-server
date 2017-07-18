package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.BaseTransactionObject;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.propscon.SerialPropsContainer;


/**
 * A base interface containing create, load and delete methods.
 * Some sub-interfaces might not always need {@link SerialGenerator}
 * and {@link TransactionMgr} in the load method.
 *
 * @author Gabor Hernadi <gabor.hernadi@linbit.com>
 */
public interface BaseDatabaseDriver<DATA>
{
    /**
     * Persists the given {@link DATA} into the database.
     *
     * The primary key for the insert statement is stored as
     * instance variables already, thus might not be retrieved from the
     * conDfnData parameter.
     *
     * @param dbCon
     *  The used database {@link Connection}
     * @param data
     *  The data to be stored (except the primary key)
     * @throws SQLException
     */
    public void create(Connection dbCon, DATA data)
        throws SQLException;

    /**
     * Removes the {@link DATA} specified by the primary key
     * stored as instance variables.
     *
     * @param dbCon
     *  The used database {@link Connection}
     * @throws SQLException
     */
    public void delete(Connection dbCon)
        throws SQLException;

    public static interface BasePropsDatabaseDriver<DATA> extends BaseDatabaseDriver<DATA>
    {
        /**
         * Loads the {@link DATA} specified by the primary key
         * stored as instance variables.
         *
         * @param dbCon
         *  The used database {@link Connection}
         * @param serialGen
         *  The {@link SerialGenerator}, used to initialize the {@link SerialPropsContainer}
         * @param transMgr
         *  The {@link TransactionMgr}, used to restore references, like {@link Node},
         *  {@link Resource}, and so on
         * @return
         *  An instance which contains valid references, but is not
         *  initialized yet in regards of {@link BaseTransactionObject#initialized()}
         *
         * @throws SQLException
         */
        public DATA load(
            Connection dbCon,
            SerialGenerator serialGen,
            TransactionMgr transMgr
        )
            throws SQLException;
    }

    public static interface BaseSimpleDatabaseDriver<DATA> extends BaseDatabaseDriver<DATA>
    {
        /**
         * Loads the {@link DATA} specified by the primary key
         * stored as instance variables.
         *
         * @param dbCon
         *  The used database {@link Connection}
         * @return
         *  An instance which contains valid references, but is not
         *  initialized yet in regards of {@link BaseTransactionObject#initialized()}
         *
         * @throws SQLException
         */
        public DATA load(Connection dbCon)
            throws SQLException;
    }
}
