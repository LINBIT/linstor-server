package com.linbit.linstor.core.objects;

import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Nodes;
import com.linbit.linstor.dbdrivers.etcd.ETCDEngine;
import com.linbit.linstor.dbdrivers.etcd.EtcdUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.transaction.ControllerETCDTransactionMgr;
import com.linbit.linstor.transaction.TransactionMgrETCD;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

/**
 * <p>
 * Special ETCD driver (although {@link NodeDbDriver} can be linked to be used for both, ETCD and SQL)
 * to prevent "duplicate key given in txn request" exception.
 * </p>
 * <p>
 * That exception WILL happen when we are declaring a Node as lost, as we first update the node
 * and afterwards delete it. If all goes well, both of this operations happen in the same request. <br/>
 * The result is an put to LINSTOR/NODES/nodeName/COLUMN, and a delete to LINSTOR/NODES/nodeName (this triggers
 * the "duplicate key given in txn request" exception).
 * </p>
 * <p>
 * To prevent this, this driver simply does not use the ranged delete request (i.e. not deleting
 * LINSTOR/NODES/nodeName), instead this driver deletes all columns one by one and let
 * {@link ControllerETCDTransactionMgr#removeDuplucateRequests} clean up the issue, preventing the exception
 * </p>
 */
@Singleton
public class NodeETCDDriver extends NodeDbDriver
{
    private ETCDEngine etcdEngine;

    @Inject
    public NodeETCDDriver(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext dbCtxRef,
        ETCDEngine etcdEngineRef,
        Provider<TransactionMgrETCD> transMgrProviderRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef
    )
    {
        super(
            errorReporterRef,
            dbCtxRef,
            etcdEngineRef,
            transMgrProviderRef,
            objProtDriverRef,
            propsContainerFactoryRef,
            transObjFactoryRef
        );
        etcdEngine = etcdEngineRef;
    }

    /**
     * Special delete method for ETCD to prevent ranged delete operation.
     * See javadoc of {@link NodeETCDDriver} above for more details
     */
    @Override
    public void delete(Node node) throws DatabaseException
    {
        // DO NOT USE ranged delete here! see java-doc of this class

        String pk = node.getName().value;
        for (Column col : Nodes.ALL)
        {
            if (!col.isPk())
            {
                etcdEngine.namespace(EtcdUtils.buildKey(col, pk)).delete(false);
            }
        }
    }
}
