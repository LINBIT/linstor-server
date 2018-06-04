package com.linbit.linstor;

import com.linbit.linstor.api.pojo.SnapshotPojo;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Provider;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;

public class SnapshotData extends BaseTransactionObject implements Snapshot
{
    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    private final SnapshotDefinition snapshotDfn;

    // Reference to the node this resource is assigned to
    private final Node node;

    // State flags
    private final StateFlags<SnapshotFlags> flags;

    private boolean suspendResource;

    private boolean takeSnapshot;

    public SnapshotData(
        UUID objIdRef,
        SnapshotDefinition snapshotDfnRef,
        Node nodeRef,
        long initFlags,
        SnapshotDataDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);

        objId = objIdRef;
        snapshotDfn = snapshotDfnRef;
        node = nodeRef;

        dbgInstanceId = UUID.randomUUID();

        flags = transObjFactory.createStateFlagsImpl(
            snapshotDfn.getResourceDefinition().getObjProt(),
            this,
            SnapshotFlags.class,
            dbDriverRef.getStateFlagsPersistence(),
            initFlags
        );

        transObjs = Arrays.asList(
            snapshotDfn,
            node,
            flags
        );
    }

    @Override
    public UUID getUuid()
    {
        return objId;
    }

    @Override
    public SnapshotDefinition getSnapshotDefinition()
    {
        return snapshotDfn;
    }

    @Override
    public Node getNode()
    {
        return node;
    }

    @Override
    public StateFlags<SnapshotFlags> getFlags()
    {
        return flags;
    }

    @Override
    public void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        snapshotDfn.getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.USE);
        getFlags().enableFlags(accCtx, SnapshotFlags.DELETE);
    }

    @Override
    public boolean getSuspendResource()
    {
        return suspendResource;
    }

    @Override
    public void setSuspendResource(boolean suspendResourceRef)
    {
        suspendResource = suspendResourceRef;
    }

    @Override
    public boolean getTakeSnapshot()
    {
        return takeSnapshot;
    }

    @Override
    public void setTakeSnapshot(boolean takeSnapshotRef)
    {
        takeSnapshot = takeSnapshotRef;
    }

    @Override
    public String toString()
    {
        return "Node: '" + node.getName() + "', " + snapshotDfn;
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    @Override
    public SnapshotApi getApiData(AccessContext accCtx, Long fullSyncId, Long updateId)
        throws AccessDeniedException
    {
        return new SnapshotPojo(
            snapshotDfn.getApiData(accCtx),
            objId,
            flags.getFlagsBits(accCtx),
            suspendResource,
            takeSnapshot,
            fullSyncId,
            updateId
        );
    }
}
