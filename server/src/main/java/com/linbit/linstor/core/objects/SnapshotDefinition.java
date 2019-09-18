package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.api.pojo.SnapshotDfnListItemPojo;
import com.linbit.linstor.api.pojo.SnapshotDfnPojo;
import com.linbit.linstor.core.apis.SnapshotDefinitionApi;
import com.linbit.linstor.core.apis.SnapshotDefinitionListItemApi;
import com.linbit.linstor.core.apis.SnapshotVolumeDefinitionApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDefinitionDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

public class SnapshotDefinition extends BaseTransactionObject implements DbgInstanceUuid, Comparable<SnapshotDefinition>
{
    public static interface InitMaps
    {
        Map<NodeName, Snapshot> getSnapshotMap();
        Map<VolumeNumber, SnapshotVolumeDefinition> getSnapshotVolumeDefinitionMap();
    }

    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    // Reference to the resource definition
    private final ResourceDefinition resourceDfn;

    private final SnapshotName snapshotName;

    private final SnapshotDefinitionDatabaseDriver dbDriver;

    // Properties container for this snapshot definition
    private final Props snapshotDfnProps;

    // State flags
    private final StateFlags<Flags> flags;

    private final TransactionMap<VolumeNumber, SnapshotVolumeDefinition> snapshotVolumeDefinitionMap;

    private final TransactionMap<NodeName, Snapshot> snapshotMap;

    private final TransactionSimpleObject<SnapshotDefinition, Boolean> deleted;

    // Not persisted because we do not resume snapshot creation after a restart
    private TransactionSimpleObject<SnapshotDefinition, Boolean> inCreation;

    public SnapshotDefinition(
        UUID objIdRef,
        ResourceDefinition resourceDfnRef,
        SnapshotName snapshotNameRef,
        long initFlags,
        SnapshotDefinitionDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        PropsContainerFactory propsContainerFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        Map<VolumeNumber, SnapshotVolumeDefinition> snapshotVlmDfnMapRef,
        Map<NodeName, Snapshot> snapshotMapRef
    )
        throws DatabaseException
    {
        super(transMgrProviderRef);
        objId = objIdRef;
        resourceDfn = resourceDfnRef;
        snapshotName = snapshotNameRef;
        dbDriver = dbDriverRef;

        dbgInstanceId = UUID.randomUUID();

        snapshotDfnProps = propsContainerFactory.getInstance(
            PropsContainer.buildPath(resourceDfn.getName(), snapshotName)
        );

        flags = transObjFactory.createStateFlagsImpl(
            resourceDfnRef.getObjProt(),
            this,
            Flags.class,
            dbDriverRef.getStateFlagsPersistence(),
            initFlags
        );

        snapshotVolumeDefinitionMap = transObjFactory.createTransactionMap(snapshotVlmDfnMapRef, null);

        snapshotMap = transObjFactory.createTransactionMap(snapshotMapRef, null);

        deleted = transObjFactory.createTransactionSimpleObject(this, Boolean.FALSE, null);

        inCreation = transObjFactory.createTransactionSimpleObject(this, Boolean.FALSE, null);

        transObjs = Arrays.asList(
            resourceDfn,
            snapshotVolumeDefinitionMap,
            snapshotMap,
            flags,
            deleted,
            inCreation
        );
    }

    public UUID getUuid()
    {
        return objId;
    }

    public ResourceDefinition getResourceDefinition()
    {
        checkDeleted();
        return resourceDfn;
    }

    public SnapshotName getName()
    {
        checkDeleted();
        return snapshotName;
    }

    public SnapshotVolumeDefinition getSnapshotVolumeDefinition(
        AccessContext accCtx,
        VolumeNumber volumeNumber
    )
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return snapshotVolumeDefinitionMap.get(volumeNumber);
    }

    public void addSnapshotVolumeDefinition(
        AccessContext accCtx,
        SnapshotVolumeDefinition snapshotVolumeDefinition
    )
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
        snapshotVolumeDefinitionMap.put(snapshotVolumeDefinition.getVolumeNumber(), snapshotVolumeDefinition);
    }

    public void removeSnapshotVolumeDefinition(
        AccessContext accCtx,
        VolumeNumber volumeNumber
    )
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
        snapshotVolumeDefinitionMap.remove(volumeNumber);
    }

    public Collection<SnapshotVolumeDefinition> getAllSnapshotVolumeDefinitions(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return snapshotVolumeDefinitionMap.values();
    }

    public Snapshot getSnapshot(AccessContext accCtx, NodeName clNodeName)
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return snapshotMap.get(clNodeName);
    }

    public Collection<Snapshot> getAllSnapshots(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return snapshotMap.values();
    }

    public void addSnapshot(AccessContext accCtx, Snapshot snapshotRef)
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
        snapshotMap.put(snapshotRef.getNodeName(), snapshotRef);
    }

    public void removeSnapshot(AccessContext accCtx, Snapshot snapshotRef)
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
        snapshotMap.remove(snapshotRef.getNodeName());
    }

    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, resourceDfn.getObjProt(), snapshotDfnProps);
    }

    public StateFlags<Flags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    public void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
    {
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.USE);
        getFlags().enableFlags(accCtx, Flags.DELETE);
    }

    public void delete(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CONTROL);

            if (!snapshotMap.isEmpty())
            {
                throw new ImplementationError("Cannot delete snapshot definition which contains snapshots");
            }

            resourceDfn.removeSnapshotDfn(accCtx, snapshotName);

            // Shallow copy the volume collection because calling delete results in elements being removed from it
            Collection<SnapshotVolumeDefinition> snapshotVolumeDefinitions =
                new ArrayList<>(snapshotVolumeDefinitionMap.values());
            for (SnapshotVolumeDefinition snapshotVolumeDefinition : snapshotVolumeDefinitions)
            {
                snapshotVolumeDefinition.delete(accCtx);
            }

            snapshotDfnProps.delete();

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(Boolean.TRUE);
        }
    }

    public boolean isDeleted()
    {
        return deleted.get();
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted snapshot definition");
        }
    }

    /**
     * Is the snapshot being used for a linstor action such as creation, deletion or rollback?
     *
     * @param accCtx
     */
    public boolean getInProgress(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();

        return inCreation.get() ||
            flags.isSet(accCtx, Flags.DELETE);
    }

    public void setInCreation(AccessContext accCtx, boolean inCreationRef)
        throws DatabaseException, AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CONTROL);
        inCreation.set(inCreationRef);
    }

    public SnapshotDefinitionApi getApiData(AccessContext accCtx)
        throws AccessDeniedException
    {
        List<SnapshotVolumeDefinitionApi> snapshotVlmDfns = new ArrayList<>();

        for (SnapshotVolumeDefinition snapshotVolumeDefinition : snapshotVolumeDefinitionMap.values())
        {
            snapshotVlmDfns.add(snapshotVolumeDefinition.getApiData(accCtx));
        }

        return new SnapshotDfnPojo(
            resourceDfn.getApiData(accCtx),
            objId,
            snapshotName.getDisplayName(),
            snapshotVlmDfns,
            flags.getFlagsBits(accCtx),
            new TreeMap<>(getProps(accCtx).map())
        );
    }

    public SnapshotDefinitionListItemApi getListItemApiData(AccessContext accCtx)
        throws AccessDeniedException
    {
        return new SnapshotDfnListItemPojo(
            getApiData(accCtx),
            snapshotMap.values().stream()
                .map(Snapshot::getNodeName)
                .map(NodeName::getDisplayName)
                .collect(Collectors.toList())
        );
    }

    @Override
    public String toString()
    {
        return "Rsc: '" + getResourceName() + "', " +
            "Snapshot: '" + snapshotName + "'";
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    @Override
    public int compareTo(SnapshotDefinition otherSnapshotDfn)
    {
        int eq = getResourceDefinition().compareTo(otherSnapshotDfn.getResourceDefinition());
        if (eq == 0)
        {
            eq = getName().compareTo(otherSnapshotDfn.getName());
        }
        return eq;
    }

    /**
     * Identifies a snapshot within a node.
     */
    public static class Key implements Comparable<Key>
    {
        private final ResourceName resourceName;

        private final SnapshotName snapshotName;

        public Key(SnapshotDefinition snapshotDefinition)
        {
            this(snapshotDefinition.getResourceName(), snapshotDefinition.getName());
        }

        public Key(ResourceName resourceNameRef, SnapshotName snapshotNameRef)
        {
            resourceName = resourceNameRef;
            snapshotName = snapshotNameRef;
        }

        public ResourceName getResourceName()
        {
            return resourceName;
        }

        public SnapshotName getSnapshotName()
        {
            return snapshotName;
        }

        // Code style exception: Automatically generated code
        @Override
        @SuppressWarnings(
            {
                "DescendantToken", "ParameterName"
            }
        )
        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            Key that = (Key) o;
            return Objects.equals(resourceName, that.resourceName) &&
                Objects.equals(snapshotName, that.snapshotName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(resourceName, snapshotName);
        }

        @Override
        @SuppressWarnings("unchecked")
        public int compareTo(Key other)
        {
            int eq = resourceName.compareTo(other.resourceName);
            if (eq == 0)
            {
                eq = snapshotName.compareTo(other.snapshotName);
            }
            return eq;
        }
    }

    public ResourceName getResourceName()
    {
        return resourceDfn.getName();
    }
    
    public enum Flags implements com.linbit.linstor.stateflags.Flags
    {
        SUCCESSFUL(1L), FAILED_DEPLOYMENT(2L), FAILED_DISCONNECT(4L), DELETE(8L);

        public final long flagValue;

        Flags(long value)
        {
            flagValue = value;
        }

        @Override
        public long getFlagValue()
        {
            return flagValue;
        }

        public static Flags[] restoreFlags(long snapshotDfnFlags)
        {
            List<Flags> flagList = new ArrayList<>();
            for (Flags flag : Flags.values())
            {
                if ((snapshotDfnFlags & flag.flagValue) == flag.flagValue)
                {
                    flagList.add(flag);
                }
            }
            return flagList.toArray(new Flags[flagList.size()]);
        }

        public static List<String> toStringList(long flagsMask)
        {
            return FlagsHelper.toStringList(Flags.class, flagsMask);
                }

        public static long fromStringList(List<String> listFlags)
        {
            return FlagsHelper.fromStringList(Flags.class, listFlags);
        }
    }

}
