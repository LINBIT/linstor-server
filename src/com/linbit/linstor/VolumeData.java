package com.linbit.linstor;

import com.linbit.linstor.api.pojo.VlmPojo;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDataDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.UUID;

import javax.inject.Provider;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class VolumeData extends BaseTransactionObject implements Volume
{
    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    // Reference to the resource this volume belongs to
    private final Resource resource;

    // Reference to the resource definition that defines the resource this volume belongs to
    private final ResourceDefinition resourceDfn;

    // Reference to the volume definition that defines this volume
    private final VolumeDefinition volumeDfn;

    private final StorPool storPool;

    // Properties container for this volume
    private final Props volumeProps;

    // State flags
    private final StateFlags<VlmFlags> flags;

    private final TransactionMap<Volume, VolumeConnection> volumeConnections;

    private String blockDevicePath;

    private String metaDiskPath;

    private final VolumeDataDatabaseDriver dbDriver;

    private final TransactionSimpleObject<VolumeData, Boolean> deleted;

    VolumeData(
        UUID uuid,
        AccessContext accCtx,
        Resource resRef,
        VolumeDefinition volDfnRef,
        StorPool storPoolRef,
        String blockDevicePathRef,
        String metaDiskPathRef,
        long initFlags,
        VolumeDataDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef
    )
        throws SQLException, AccessDeniedException
    {
        super(transMgrProviderRef);

        objId = uuid;
        dbgInstanceId = UUID.randomUUID();
        resource = resRef;
        resourceDfn = resRef.getDefinition();
        volumeDfn = volDfnRef;
        storPool = storPoolRef;
        blockDevicePath = blockDevicePathRef;
        metaDiskPath = metaDiskPathRef;
        dbDriver = dbDriverRef;

        flags = transObjFactory.createStateFlagsImpl(
            resRef.getObjProt(),
            this,
            VlmFlags.class,
            this.dbDriver.getStateFlagsPersistence(),
            initFlags
        );

        volumeConnections = transObjFactory.createTransactionMap(new HashMap<Volume, VolumeConnection>(), null);
        volumeProps = propsContainerFactory.getInstance(
            PropsContainer.buildPath(
                resRef.getAssignedNode().getName(),
                resRef.getDefinition().getName(),
                volDfnRef.getVolumeNumber()
            )
        );
        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs = Arrays.asList(
            resource,
            volumeDfn,
            storPool,
            volumeConnections,
            volumeProps,
            flags,
            deleted
        );

        ((ResourceData) resRef).putVolume(accCtx, this);
        ((StorPoolData) storPoolRef).putVolume(accCtx, this);
        ((VolumeDefinitionData) volDfnRef).putVolume(accCtx, this);
        activateTransMgr();
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    @Override
    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    @Override
    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, resource.getObjProt(), volumeProps);
    }

    @Override
    public Resource getResource()
    {
        checkDeleted();
        return resource;
    }

    @Override
    public ResourceDefinition getResourceDefinition()
    {
        checkDeleted();
        return resourceDfn;
    }

    @Override
    public VolumeDefinition getVolumeDefinition()
    {
        checkDeleted();
        return volumeDfn;
    }

    @Override
    public VolumeConnection getVolumeConnection(AccessContext accCtx, Volume othervolume)
        throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return volumeConnections.get(othervolume);
    }

    @Override
    public void setVolumeConnection(AccessContext accCtx, VolumeConnection volumeConnection)
        throws AccessDeniedException
    {
        checkDeleted();

        Volume sourceVolume = volumeConnection.getSourceVolume(accCtx);
        Volume targetVolume = volumeConnection.getTargetVolume(accCtx);

        sourceVolume.getResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        targetVolume.getResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        if (this == sourceVolume)
        {
            volumeConnections.put(targetVolume, volumeConnection);
        }
        else
        {
            volumeConnections.put(sourceVolume, volumeConnection);
        }
    }

    @Override
    public void removeVolumeConnection(AccessContext accCtx, VolumeConnection volumeConnection)
        throws AccessDeniedException
    {
        checkDeleted();

        Volume sourceVolume = volumeConnection.getSourceVolume(accCtx);
        Volume targetVolume = volumeConnection.getTargetVolume(accCtx);

        sourceVolume.getResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        targetVolume.getResource().getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        if (this == sourceVolume)
        {
            volumeConnections.remove(targetVolume);
        }
        else
        {
            volumeConnections.remove(sourceVolume);
        }
    }

    @Override
    public StorPool getStorPool(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return storPool;
    }

    @Override
    public StateFlags<VlmFlags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    @Override
    public String getBlockDevicePath(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return blockDevicePath;
    }

    @Override
    public String getMetaDiskPath(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return metaDiskPath;
    }

    @Override
    public void setBlockDevicePath(AccessContext accCtx, String path) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        blockDevicePath = path;
    }

    @Override
    public void setMetaDiskPath(AccessContext accCtx, String path) throws AccessDeniedException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        metaDiskPath = path;
    }

    @Override
    public void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        resource.getObjProt().requireAccess(accCtx, AccessType.USE);
        getFlags().enableFlags(accCtx, Volume.VlmFlags.DELETE);
    }


    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        if (!deleted.get())
        {
            resource.getObjProt().requireAccess(accCtx, AccessType.USE);

            // preventing ConcurrentModificationException
            Collection<VolumeConnection> values = new ArrayList<>(volumeConnections.values());
            for (VolumeConnection vlmConn : values)
            {
                vlmConn.delete(accCtx);
            }

            ((ResourceData) resource).removeVolume(accCtx, this);
            storPool.removeVolume(accCtx, this);
            ((VolumeDefinitionData) volumeDfn).removeVolume(accCtx, this);

            volumeProps.delete();

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted volume");
        }
    }

    @Override
    public String toString()
    {
        return "Node: '" + resource.getAssignedNode().getName() + "', " +
               "Rsc: '" + resource.getDefinition().getName() + "', " +
               "VlmNr: '" + volumeDfn.getVolumeNumber() + "'";
    }

    @Override
    public Volume.VlmApi getApiData(AccessContext accCtx) throws AccessDeniedException
    {
        return new VlmPojo(
                getStorPool(accCtx).getName().getDisplayName(),
                getStorPool(accCtx).getUuid(),
                getVolumeDefinition().getUuid(),
                getUuid(),
                getBlockDevicePath(accCtx),
                getMetaDiskPath(accCtx),
                getVolumeDefinition().getVolumeNumber().value,
                getVolumeDefinition().getMinorNr(accCtx).value,
                getFlags().getFlagsBits(accCtx),
                getProps(accCtx).map(),
                getStorPool(accCtx).getDriverName(),
                getStorPool(accCtx).getDefinition(accCtx).getUuid(),
                getStorPool(accCtx).getDefinition(accCtx).getProps(accCtx).map(),
                getStorPool(accCtx).getProps(accCtx).map()
        );
    }
}
