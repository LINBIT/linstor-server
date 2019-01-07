package com.linbit.linstor;

import com.linbit.ErrorCheck;
import com.linbit.ValueInUseException;
import com.linbit.linstor.api.pojo.RscDfnPojo;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.layer.data.categories.RscDfnLayerData;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.locks.LockGuard;
import com.linbit.utils.Pair;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Provider;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ResourceDefinitionData extends BaseTransactionObject implements ResourceDefinition
{
    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    // Resource name
    private final ResourceName resourceName;

    // Tcp Port
    // TODO: should be moved to DrbdRscData once controller knows about it
    private final TransactionSimpleObject<ResourceDefinitionData, TcpPortNumber> port;

    private final DynamicNumberPool tcpPortPool;

    // Volumes of the resource
    private final TransactionMap<VolumeNumber, VolumeDefinition> volumeMap;

    // Resources defined by this ResourceDefinition
    // TODO: change to something like Map<NodeName, Map<ResourceType, Resource>>
    private final TransactionMap<Pair<NodeName, ResourceType>, Resource> resourceMap;

    // Snapshots from this resource definition
    private final TransactionMap<SnapshotName, SnapshotDefinition> snapshotDfnMap;

    // State flags
    private final StateFlags<RscDfnFlags> flags;

    // TODO: should be moved to DrbdRscData once controller knows about it
    private final TransactionSimpleObject<ResourceDefinitionData, TransportType> transportType;

    // Object access controls
    private final ObjectProtection objProt;

    // Properties container for this resource definition
    private final Props rscDfnProps;

    // TODO: should be moved to DrbdRscData once controller knows about it
    private final String secret;

    private final ResourceDefinitionDataDatabaseDriver dbDriver;

    // TODO: should be moved to DrbdRscData once controller knows about it
    private final TransactionSimpleObject<ResourceDefinitionData, Boolean> down;

    private final TransactionMap<Class<? extends RscDfnLayerData>, RscDfnLayerData> layerDataMap;

    private final TransactionSimpleObject<ResourceDefinitionData, Boolean> deleted;

    ResourceDefinitionData(
        UUID objIdRef,
        ObjectProtection objProtRef,
        ResourceName resName,
        TcpPortNumber portRef,
        DynamicNumberPool tcpPortPoolRef,
        long initialFlags,
        String secretRef,
        TransportType transTypeRef,
        ResourceDefinitionDataDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef,
        Map<VolumeNumber, VolumeDefinition> vlmDfnMapRef,
        Map<Pair<NodeName, ResourceType>, Resource> rscMapRef,
        Map<SnapshotName, SnapshotDefinition> snapshotDfnMapRef,
        Map<Class<? extends RscDfnLayerData>, RscDfnLayerData> layerDataMapRef
    )
        throws SQLException
    {
        super(transMgrProviderRef);

        ErrorCheck.ctorNotNull(ResourceDefinitionData.class, ResourceName.class, resName);
        ErrorCheck.ctorNotNull(ResourceDefinitionData.class, ObjectProtection.class, objProtRef);
        objId = objIdRef;
        dbgInstanceId = UUID.randomUUID();
        objProt = objProtRef;
        resourceName = resName;
        secret = secretRef;
        dbDriver = dbDriverRef;
        tcpPortPool = tcpPortPoolRef;

        port = transObjFactory.createTransactionSimpleObject(
            this,
            portRef,
            this.dbDriver.getPortDriver()
        );
        volumeMap = transObjFactory.createTransactionMap(vlmDfnMapRef, null);
        resourceMap = transObjFactory.createTransactionMap(rscMapRef, null);
        snapshotDfnMap = transObjFactory.createTransactionMap(snapshotDfnMapRef, null);
        down = transObjFactory.createTransactionSimpleObject(this, false, null);
        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        rscDfnProps = propsContainerFactory.getInstance(
            PropsContainer.buildPath(resName)
        );
        flags = transObjFactory.createStateFlagsImpl(
            objProt,
            this,
            RscDfnFlags.class,
            dbDriver.getStateFlagsPersistence(),
            initialFlags
        );

        transportType = transObjFactory.createTransactionSimpleObject(
            this,
            transTypeRef,
            this.dbDriver.getTransportTypeDriver()
        );

        layerDataMap = transObjFactory.createTransactionMap(layerDataMapRef, null); // TODO: create dbdriver

        transObjs = Arrays.asList(
            flags,
            objProt,
            volumeMap,
            resourceMap,
            rscDfnProps,
            port,
            transportType,
            layerDataMap,
            deleted
        );
    }

    @Override
    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    @Override
    public ResourceName getName()
    {
        checkDeleted();
        return resourceName;
    }

    @Override
    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, objProt, rscDfnProps);
    }

    synchronized void putVolumeDefinition(AccessContext accCtx, VolumeDefinition volDfn)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        volumeMap.put(volDfn.getVolumeNumber(), volDfn);
    }

    synchronized void removeVolumeDefinition(AccessContext accCtx, VolumeDefinition volDfn)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        volumeMap.remove(volDfn.getVolumeNumber());
    }

    @Override
    public int getVolumeDfnCount(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return volumeMap.size();
    }

    @Override
    public VolumeDefinition getVolumeDfn(AccessContext accCtx, VolumeNumber volNr)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return volumeMap.get(volNr);
    }

    @Override
    public Iterator<VolumeDefinition> iterateVolumeDfn(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return volumeMap.values().iterator();
    }

    @Override
    public Stream<VolumeDefinition> streamVolumeDfn(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return volumeMap.values().stream();
    }

    @Override
    public int getResourceCount()
    {
        return resourceMap.size();
    }

    @Override
    public int diskfullCount(AccessContext accCtx) throws AccessDeniedException
    {
        int count = 0;
        for (Resource rsc : streamResource(accCtx).collect(Collectors.toList()))
        {
            if (rsc.getStateFlags().isUnset(accCtx, Resource.RscFlags.DISKLESS))
            {
                count++;
            }
        }
        return count;
    }

    @Override
    public Iterator<Resource> iterateResource(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return resourceMap.values().iterator();
    }

    @Override
    public Stream<Resource> streamResource(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return resourceMap.values().stream();
    }

    @Override
    public void copyResourceMap(
        AccessContext accCtx, Map<Pair<NodeName, ResourceType>, ? super Resource> dstMap
    )
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        dstMap.putAll(resourceMap);
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return objProt;
    }

    @Override
    public Resource getResource(AccessContext accCtx, NodeName clNodeName, ResourceType type)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return resourceMap.get(new Pair<>(clNodeName, type));
    }

    @Override
    public TcpPortNumber getPort(AccessContext accCtx) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return port.get();
    }

    @Override
    public TcpPortNumber setPort(AccessContext accCtx, TcpPortNumber portNr)
        throws AccessDeniedException, SQLException, ValueInUseException
    {
        objProt.requireAccess(accCtx, AccessType.USE);
        if (tcpPortPool != null)
        {
            tcpPortPool.deallocate(port.get().value);
            tcpPortPool.allocate(portNr.value);
        }
        return this.port.set(portNr);
    }

    @Override
    public void addResource(AccessContext accCtx, Resource resRef) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);

        resourceMap.put(new Pair<>(resRef.getAssignedNode().getName(), resRef.getType()), resRef);
    }

    void removeResource(AccessContext accCtx, Resource resRef) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);

        resourceMap.remove(new Pair<>(resRef.getAssignedNode().getName(), resRef.getType()));
    }

    @Override
    public void addSnapshotDfn(AccessContext accCtx, SnapshotDefinition snapshotDfn)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);

        snapshotDfnMap.put(snapshotDfn.getName(), snapshotDfn);
    }

    @Override
    public SnapshotDefinition getSnapshotDfn(AccessContext accCtx, SnapshotName snapshotName)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return snapshotDfnMap.get(snapshotName);
    }

    @Override
    public Collection<SnapshotDefinition> getSnapshotDfns(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return snapshotDfnMap.values();
    }

    @Override
    public void removeSnapshotDfn(AccessContext accCtx, SnapshotName snapshotName)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);

        snapshotDfnMap.remove(snapshotName);
    }

    @Override
    public StateFlags<RscDfnFlags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    @Override
    public void setDown(AccessContext accCtx, boolean downRef)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);

        down.set(downRef);
    }

    @Override
    public boolean isDown(AccessContext accCtx)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return down.get();
    }

    @Override
    public String getSecret(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return secret;
    }

    @Override
    public TransportType getTransportType(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return transportType.get();
    }

    @Override
    public TransportType setTransportType(AccessContext accCtx, TransportType type)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        return transportType.set(type);
    }

    @Override
    public boolean hasDiskless(AccessContext accCtx) throws AccessDeniedException
    {
        boolean hasDiskless = false;
        for (Resource rsc : streamResource(accCtx).collect(Collectors.toList()))
        {
            hasDiskless = rsc.getStateFlags().isSet(accCtx, Resource.RscFlags.DISKLESS);
            if (hasDiskless)
            {
                break;
            }
        }
        return hasDiskless;
    }

    @Override
    public void markDeleted(AccessContext accCtx) throws AccessDeniedException, SQLException
    {
        getFlags().enableFlags(accCtx, RscDfnFlags.DELETE);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends RscDfnLayerData> T setLayerData(AccessContext accCtx, T rscDfnLayerData)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        Class<? extends RscDfnLayerData> clazz = rscDfnLayerData.getClass();
        T ret = (T) layerDataMap.get(clazz);
        layerDataMap.put(clazz, rscDfnLayerData);
        return ret;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends RscDfnLayerData> T getLayerData(AccessContext accCtx, Class<T> clazz)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        RscDfnLayerData obj = layerDataMap.get(clazz);
        clazz.cast(obj);
        return (T) obj;
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        if (!deleted.get())
        {
            objProt.requireAccess(accCtx, AccessType.CONTROL);

            // Shallow copy the resource collection because calling delete results in elements being removed from it
            Collection<Resource> resources = new ArrayList<>(resourceMap.values());
            for (Resource rsc : resources)
            {
                rsc.delete(accCtx);
            }

            // Shallow copy the volume definition collection because calling delete results in elements being removed
            // from it
            Collection<VolumeDefinition> volumeDefinitions = new ArrayList<>(volumeMap.values());
            for (VolumeDefinition volumeDefinition : volumeDefinitions)
            {
                volumeDefinition.delete(accCtx);
            }

            rscDfnProps.delete();

            if (tcpPortPool != null)
            {
                tcpPortPool.deallocate(port.get().value);
            }

            objProt.delete(accCtx);

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted resource definition");
        }
    }

    @Override
    public ResourceDefinition.RscDfnApi getApiData(AccessContext accCtx)
        throws AccessDeniedException
    {
        ArrayList<VolumeDefinition.VlmDfnApi> vlmDfnList = new ArrayList<>();
        Iterator<VolumeDefinition> vlmDfnIter = iterateVolumeDfn(accCtx);
        while (vlmDfnIter.hasNext())
        {
            VolumeDefinition vd = vlmDfnIter.next();
            vlmDfnList.add(vd.getApiData(accCtx));
        }
        return new RscDfnPojo(
            getUuid(),
            getName().getDisplayName(),
            getPort(accCtx).value,
            getSecret(accCtx),
            getFlags().getFlagsBits(accCtx),
            getTransportType(accCtx).name(),
            isDown(accCtx),
            getProps(accCtx).map(),
            vlmDfnList
        );
    }

    @Override
    public Optional<Resource> anyResourceInUse(AccessContext accCtx)
        throws AccessDeniedException
    {
        Resource rscInUse = null;
        Iterator<Resource> rscInUseIterator = iterateResource(accCtx);
        while (rscInUseIterator.hasNext() && rscInUse == null)
        {
            Resource rsc = rscInUseIterator.next();

            Peer nodePeer = rsc.getAssignedNode().getPeer(accCtx);
            if (nodePeer != null)
            {
                Boolean inUse;
                try (LockGuard ignored = LockGuard.createLocked(nodePeer.getSatelliteStateLock().readLock()))
                {
                    inUse = nodePeer.getSatelliteState().getFromResource(resourceName, SatelliteResourceState::isInUse);
                }
                if (inUse != null && inUse)
                {
                    rscInUse = rsc;
                }
            }
        }
        return Optional.ofNullable(rscInUse);
    }

    @Override
    public String toString()
    {
        return "Rsc: '" + resourceName + "'";
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }
}
