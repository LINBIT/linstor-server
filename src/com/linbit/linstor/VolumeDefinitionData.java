package com.linbit.linstor;

import com.linbit.Checks;
import com.linbit.ErrorCheck;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MdException;
import com.linbit.drbd.md.MetaData;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.linstor.api.pojo.VlmDfnPojo;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.Identity;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Stream;

import javax.inject.Provider;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class VolumeDefinitionData extends BaseTransactionObject implements VolumeDefinition
{
    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    // Resource definition this VolumeDefinition belongs to
    private final ResourceDefinition resourceDfn;

    // DRBD volume number
    private final VolumeNumber volumeNr;

    // DRBD device minor number
    private final TransactionSimpleObject<VolumeDefinitionData, MinorNumber> minorNr;

    private final DynamicNumberPool minorNrPool;

    // Net volume size in kiB
    private final TransactionSimpleObject<VolumeDefinitionData, Long> volumeSize;

    // Properties container for this volume definition
    private final Props vlmDfnProps;

    // State flags
    private final StateFlags<VlmDfnFlags> flags;

    private final TransactionMap<String, Volume> volumes;

    private final VolumeDefinitionDataDatabaseDriver dbDriver;

    private final TransactionSimpleObject<VolumeDefinitionData, Boolean> deleted;

    private transient TransactionSimpleObject<VolumeDefinitionData, String> cryptKey;

    VolumeDefinitionData(
        UUID uuid,
        AccessContext accCtx,
        ResourceDefinition resDfnRef,
        VolumeNumber volNr,
        MinorNumber minor,
        DynamicNumberPool minorNrPoolRef,
        long volSize,
        long initFlags,
        VolumeDefinitionDataDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef
    )
        throws MdException, AccessDeniedException, SQLException
    {
        super(transMgrProviderRef);
        ErrorCheck.ctorNotNull(VolumeDefinitionData.class, ResourceDefinition.class, resDfnRef);
        ErrorCheck.ctorNotNull(VolumeDefinitionData.class, VolumeNumber.class, volNr);
        ErrorCheck.ctorNotNull(VolumeDefinitionData.class, MinorNumber.class, minor);

        // Creating a new volume definition requires CHANGE access to the resource definition
        resDfnRef.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        try
        {
            Checks.genericRangeCheck(
                volSize, MetaData.DRBD_MIN_NET_kiB, MetaData.DRBD_MAX_kiB,
                "Volume size value %d is out of range [%d - %d]"
            );
        }
        catch (ValueOutOfRangeException valueExc)
        {
            String excMessage = String.format(
                "Volume size value %d is out of range [%d - %d]",
                volSize, MetaData.DRBD_MIN_NET_kiB, MetaData.DRBD_MAX_kiB
            );
            if (valueExc.getViolationType() == ValueOutOfRangeException.ViolationType.TOO_LOW)
            {
                throw new MinSizeException(excMessage);
            }
            throw new MaxSizeException(excMessage);
        }

        objId = uuid;
        dbgInstanceId = UUID.randomUUID();
        resourceDfn = resDfnRef;

        dbDriver = dbDriverRef;
        minorNrPool = minorNrPoolRef;

        volumeNr = volNr;
        minorNr = transObjFactory.createTransactionSimpleObject(
            this,
            minor,
            dbDriver.getMinorNumberDriver()
        );
        volumeSize = transObjFactory.createTransactionSimpleObject(
            this,
            volSize,
            dbDriver.getVolumeSizeDriver()
        );

        vlmDfnProps = propsContainerFactory.getInstance(
            PropsContainer.buildPath(resDfnRef.getName(), volumeNr)
        );

        volumes = transObjFactory.createTransactionMap(new TreeMap<String, Volume>(), null);

        flags = transObjFactory.createStateFlagsImpl(
            resDfnRef.getObjProt(),
            this,
            VlmDfnFlags.class,
            this.dbDriver.getStateFlagsPersistence(),
            initFlags
        );


        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);
        cryptKey = transObjFactory.createTransactionSimpleObject(this, null, null);

        transObjs = Arrays.asList(
            vlmDfnProps,
            resourceDfn,
            minorNr,
            volumeSize,
            flags,
            deleted,
            cryptKey
        );

        ((ResourceDefinitionData) resourceDfn).putVolumeDefinition(accCtx, this);
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
        return PropsAccess.secureGetProps(accCtx, resourceDfn.getObjProt(), vlmDfnProps);
    }

    @Override
    public ResourceDefinition getResourceDefinition()
    {
        checkDeleted();
        return resourceDfn;
    }

    @Override
    public VolumeNumber getVolumeNumber()
    {
        checkDeleted();
        return volumeNr;
    }

    @Override
    public MinorNumber getMinorNr(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return minorNr.get();
    }

    @Override
    public MinorNumber setMinorNr(AccessContext accCtx, MinorNumber newMinorNr)
        throws AccessDeniedException, SQLException, ValueOutOfRangeException, ValueInUseException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        if (minorNrPool != null)
        {
            minorNrPool.deallocate(minorNr.get().value);
            minorNrPool.allocate(newMinorNr.value);
        }
        return minorNr.set(newMinorNr);
    }

    @Override
    public long getVolumeSize(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return volumeSize.get();
    }

    @Override
    public Long setVolumeSize(AccessContext accCtx, long newVolumeSize)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        return volumeSize.set(newVolumeSize);
    }

    @Override
    public StateFlags<VlmDfnFlags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    public void putVolume(AccessContext accCtx, VolumeData volumeData) throws AccessDeniedException
    {
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        volumes.put(getResourceId(volumeData.getResource()), volumeData);
    }

    public void removeVolume(AccessContext accCtx, VolumeData volumeData) throws AccessDeniedException
    {
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        volumes.remove(getResourceId(volumeData.getResource()));
    }

    @Override
    public Iterator<Volume> iterateVolumes(AccessContext accCtx) throws AccessDeniedException
    {
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return volumes.values().iterator();
    }

    @Override
    public Stream<Volume> streamVolumes(AccessContext accCtx) throws AccessDeniedException
    {
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return volumes.values().stream();
    }

    private String getResourceId(Resource rsc)
    {
        return rsc.getAssignedNode().getName().value + "/" +
            rsc.getDefinition().getName().value;
    }

    @Override
    public void setKey(AccessContext accCtx, String key) throws AccessDeniedException, SQLException
    {
        checkDeleted();
        if (!accCtx.subjectId.equals(Identity.SYSTEM_ID))
        {
            throw new AccessDeniedException("Only system context is allowed to set crypt key");
        }
        cryptKey.set(key);
    }

    @Override
    public String getKey(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        if (!accCtx.subjectId.equals(Identity.SYSTEM_ID))
        {
            throw new AccessDeniedException("Only system context is allowed to get crypt key");
        }
        return cryptKey.get();
    }

    @Override
    public void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CONTROL);
        getFlags().enableFlags(accCtx, VolumeDefinition.VlmDfnFlags.DELETE);
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        if (!deleted.get())
        {
            resourceDfn.getObjProt().requireAccess(accCtx, AccessType.CONTROL);

            ((ResourceDefinitionData) resourceDfn).removeVolumeDefinition(accCtx, this);

            // preventing ConcurrentModificationException
            List<Volume> vlms = new ArrayList<>(volumes.values());
            for (Volume vlm : vlms)
            {
                vlm.delete(accCtx);
            }

            vlmDfnProps.delete();

            if (minorNrPool != null)
            {
                minorNrPool.deallocate(minorNr.get().value);
            }

            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted volume definition");
        }
    }

    @Override
    public VlmDfnApi getApiData(AccessContext accCtx) throws AccessDeniedException
    {
        return new VlmDfnPojo(
            getUuid(),
            getVolumeNumber().value,
            getMinorNr(accCtx).value,
            getVolumeSize(accCtx),
            getFlags().getFlagsBits(accCtx),
            getProps(accCtx).map()
        );
    }



    @Override
    public String toString()
    {
        return "Rsc: '" + resourceDfn.getName() + "', " +
               "VlmNr: '" + volumeNr + "'";
    }
}
