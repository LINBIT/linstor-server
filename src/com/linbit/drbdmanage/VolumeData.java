package com.linbit.drbdmanage;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsAccess;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.stateflags.StateFlags;
import com.linbit.drbdmanage.stateflags.StateFlagsBits;
import com.linbit.drbdmanage.stateflags.StateFlagsPersistence;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class VolumeData extends BaseTransactionObject implements Volume
{
    // Object identifier
    private UUID objId;

    // Reference to the resource this volume belongs to
    private Resource resourceRef;

    // Reference to the resource definition that defines the resource this volume belongs to
    private ResourceDefinition resourceDfn;

    // Reference to the volume definition that defines this volume
    private VolumeDefinition volumeDfn;

    // Properties container for this volume
    private Props volumeProps;

    // State flags
    private StateFlags<VlmFlags> flags;

    private VolumeDataDatabaseDriver dbDriver;

    VolumeData(Resource resRef, VolumeDefinition volDfn)
    {
        objId = UUID.randomUUID();
        resourceRef = resRef;
        resourceDfn = resRef.getDefinition();
        volumeDfn = volDfn;

        dbDriver = DrbdManage.getVolumeDataDatabaseDriver();

        flags = new VlmFlagsImpl(
            resRef.getObjProt(),
            dbDriver.getStateFlagsPersistence()
        );

        transObjs = Arrays.asList(
            resourceRef,
            volumeDfn,
            volumeProps,
            flags
        );
    }

    public static VolumeData getInstance(
        Resource resRef,
        VolumeDefinition volDfn,
        TransactionMgr transMgr,
        boolean createIfNotExists) throws SQLException
    {
        VolumeData vol = null;

        VolumeDataDatabaseDriver driver = DrbdManage.getVolumeDataDatabaseDriver();
        boolean created = false;
        if (transMgr != null)
        {
            vol = driver.load(transMgr.dbCon, resRef, volDfn);
        }

        if (vol == null && createIfNotExists)
        {
            vol = new VolumeData(resRef, volDfn);
            created = true;
        }

        if (vol != null)
        {
            vol.initialized();
        }
        if (created && transMgr != null)
        {
            driver.create(transMgr.dbCon, vol);
        }

        return vol;
    }


    @Override
    public UUID getUuid()
    {
        return objId;
    }

    @Override
    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        return PropsAccess.secureGetProps(accCtx, resourceRef.getObjProt(), volumeProps);
    }

    @Override
    public Resource getResource()
    {
        return resourceRef;
    }

    @Override
    public ResourceDefinition getResourceDfn()
    {
        return resourceDfn;
    }

    @Override
    public VolumeDefinition getVolumeDfn()
    {
        return volumeDfn;
    }

    @Override
    public StateFlags<VlmFlags> getFlags()
    {
        return flags;
    }

    private static final class VlmFlagsImpl extends StateFlagsBits<VlmFlags>
    {
        VlmFlagsImpl(ObjectProtection objProtRef, StateFlagsPersistence persistenceRef)
        {
            super(objProtRef, StateFlagsBits.getMask(VlmFlags.ALL_FLAGS), persistenceRef);
        }
    }
}
