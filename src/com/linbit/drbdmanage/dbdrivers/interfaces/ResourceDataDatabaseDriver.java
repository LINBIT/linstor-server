package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.BaseTransactionObject;
import com.linbit.drbdmanage.ConnectionDefinitionData;
import com.linbit.drbdmanage.Node;
import com.linbit.drbdmanage.Resource.RscFlags;
import com.linbit.drbdmanage.ResourceData;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.propscon.SerialPropsContainer;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

/**
 * Database driver for {@link ResourceData}.
 *
 * @author Gabor Hernadi <gabor.hernadi@linbit.com>
 */
public interface ResourceDataDatabaseDriver extends BaseDatabaseDriver<ResourceData>
{
    /**
     * Loads the {@link ResourceData} specified by the primary key
     * stored as instance variables.
     *
     * @param dbCon
     *  The used database {@link Connection}
     * @param node
     *  The {@link Node} this resource should be assigned to
     * @param serialGen
     *  The {@link SerialGenerator}, used to initialize the {@link SerialPropsContainer}
     * @param transMgr
     *  The {@link TransactionMgr}, used to restore references, like {@link Node},
     *  {@link Resource}, and so on
     * @return
     *  A {@link ConnectionDefinitionData} which contains valid references, but is not
     *  initialized yet in regards of {@link BaseTransactionObject#initialized()}
     *
     * @throws SQLException
     */
    public ResourceData load(
        Node node,
        SerialGenerator serialGen,
        TransactionMgr transMgr
    )
        throws SQLException;

    /**
     * A special sub-driver to update the persisted {@link RscFlags}. The data record
     * is specified by the primary key stored as instance variables.
     *
     * @return
     */
    public StateFlagsPersistence getStateFlagPersistence();

    /**
     * A special sub-driver to update the instance specific {@link Props}. The data record
     * is specified by the primary key stored as instance variables.
     *
     * @return
     */
    public PropsConDatabaseDriver getPropsConDriver();

}
