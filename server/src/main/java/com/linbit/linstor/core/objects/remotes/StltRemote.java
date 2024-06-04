package com.linbit.linstor.core.objects.remotes;

import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.api.pojo.StltRemotePojo;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Provider;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Temporary object storing ip+port and other information of the target satellite.
 * This object will NOT be persisted.
 * This object is expected to be deleted after a backup shipping.
 */
public class StltRemote extends AbsRemote
{
    public interface InitMaps
    {
        // currently only a place holder for future maps
    }

    private final TransactionSimpleObject<StltRemote, String> ip;
    private final TransactionMap<StltRemote, String, Integer> ports;
    private final TransactionSimpleObject<StltRemote, Boolean> useZstd;
    private final TransactionSimpleObject<StltRemote, Boolean> deleted;
    private final StateFlags<Flags> flags;
    // this remoteName references the complete cluster instead of only the stlt the shipping should go to
    private final RemoteName linstorRemoteName;
    // a reference to the node the stltRemote is describing
    private final Node node;
    // rsc-name of the snapDfn we are shipping to or receiving from
    private final String otherRscName;

    public StltRemote(
        ObjectProtection objProtRef,
        UUID objIdRef,
        RemoteName remoteNameRef,
        long initialFlags,
        String ipRef,
        Map<String, Integer> portRef,
        Boolean useZstdRef,
        @Nullable RemoteName linstorRemoteNameRef,
        Node nodeRef,
        StateFlagsPersistence<StltRemote> stateFlagsDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider,
        @Nullable String otherRscNameRef
    )
    {
        super(objIdRef, transObjFactory, transMgrProvider, objProtRef, remoteNameRef);
        linstorRemoteName = linstorRemoteNameRef;
        node = nodeRef;
        otherRscName = otherRscNameRef;

        ip = transObjFactory.createTransactionSimpleObject(this, ipRef, null);
        ports = transObjFactory.createTransactionPrimitiveMap(this, portRef, null);
        useZstd = transObjFactory.createTransactionSimpleObject(this, useZstdRef == null ? false : useZstdRef, null);

        flags = transObjFactory.createStateFlagsImpl(
            objProt,
            this,
            Flags.class,
            stateFlagsDriverRef,
            initialFlags
        );

        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs = Arrays.asList(
            objProt,
            ip,
            ports,
            useZstd,
            flags,
            deleted
        );
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return objProt;
    }

    @Override
    public int compareTo(@Nonnull AbsRemote remote)
    {
        int cmp = remote.getClass().getSimpleName().compareTo(StltRemote.class.getSimpleName());
        if (cmp == 0)
        {
            cmp = remoteName.compareTo(remote.getName());
        }
        return cmp;
    }

    @Override
    public int hashCode()
    {
        checkDeleted();
        return Objects.hash(remoteName);
    }

    @Override
    public boolean equals(Object obj)
    {
        checkDeleted();
        boolean ret = false;
        if (this == obj)
        {
            ret = true;
        }
        else if (obj instanceof StltRemote)
        {
            StltRemote other = (StltRemote) obj;
            other.checkDeleted();
            ret = Objects.equals(remoteName, other.remoteName);
        }
        return ret;
    }

    @Override
    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    @Override
    public RemoteName getName()
    {
        checkDeleted();
        return remoteName;
    }

    public RemoteName getLinstorRemoteName()
    {
        checkDeleted();
        return linstorRemoteName;
    }

    public Node getNode()
    {
        checkDeleted();
        return node;
    }

    public String getOtherRscName()
    {
        return otherRscName;
    }

    public String getIp(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return ip.get();
    }

    public void setIp(AccessContext accCtx, String ipRef) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        ip.set(ipRef);
    }

    public Map<String, Integer> getPorts(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return ports;
    }

    public void setPort(AccessContext accCtx, int portRef, int vlmNrRef, String rscLayerSuffixRef)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        ports.put(vlmNrRef + rscLayerSuffixRef, portRef);
    }

    /**
     * This method adds the ports in the map given to the already existing ports
     */
    public void addPorts(AccessContext accCtx, Map<String, Integer> portsRef) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        ports.putAll(portsRef);
    }

    /**
     * This method will replace the already existing ports with the ports in the map given
     */
    public void setAllPorts(AccessContext accCtx, Map<String, Integer> portsRef) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        ports.clear();
        ports.putAll(portsRef);
    }

    public Boolean useZstd(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return useZstd.get();
    }

    public void useZstd(AccessContext accCtx, Boolean useZstdRef) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        useZstd.set(useZstdRef == null ? false : useZstdRef);
    }

    @Override
    public StateFlags<Flags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    @Override
    public RemoteType getType()
    {
        return RemoteType.SATELLITE;
    }

    public StltRemotePojo getApiData(AccessContext accCtx, Long fullSyncId, Long updateId) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return new StltRemotePojo(
            objId,
            remoteName.displayValue,
            flags.getFlagsBits(accCtx),
            ip.get(),
            ports,
            useZstd.get(),
            fullSyncId,
            updateId
        );
    }

    /**
     * This method removes all currently set values and replaces them with the values found in the given StltRemotePojo
     */
    public void applyApiData(AccessContext accCtx, StltRemotePojo apiData)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        ip.set(apiData.getIp());
        ports.clear();
        ports.putAll(apiData.getPorts());
        useZstd.set(apiData.useZstd());

        flags.resetFlagsTo(accCtx, Flags.restoreFlags(apiData.getFlags()));
    }

    @Override
    public boolean isDeleted()
    {
        return deleted.get();
    }

    @Override
    public void delete(AccessContext accCtx) throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            objProt.requireAccess(accCtx, AccessType.CONTROL);

            objProt.delete(accCtx);

            activateTransMgr();

            deleted.set(true);
        }
    }

    @Override
    protected void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted StltRemote");
        }
    }

    @Override
    protected String toStringImpl()
    {
        return "StltRemote '" + remoteName.displayValue + "'";
    }
}
