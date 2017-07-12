package com.linbit.drbdmanage;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsAccess;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.propscon.SerialPropsContainer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
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

    private String blockDevicePath;

    /*
     * used by getInstance
     */
    private VolumeData(
        Resource resRef,
        VolumeDefinition volDfn,
        String blockDevicePathRef,
        long initFlags,
        SerialGenerator srlGen,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        this(
            UUID.randomUUID(),
            resRef,
            volDfn,
            blockDevicePathRef,
            initFlags,
            srlGen,
            transMgr
        );
    }

    /*
     * used by database drivers and tests
     */
    VolumeData(
        UUID uuid,
        Resource resRef,
        VolumeDefinition volDfnRef,
        String blockDevicePathRef,
        long initFlags,
        SerialGenerator srlGen,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        objId = uuid;
        resourceRef = resRef;
        resourceDfn = resRef.getDefinition();
        volumeDfn = volDfnRef;
        blockDevicePath = blockDevicePathRef;

        VolumeDataDatabaseDriver dbDriver = DrbdManage.getVolumeDataDatabaseDriver(resRef, volDfnRef);

        flags = new VlmFlagsImpl(
            resRef.getObjProt(),
            dbDriver.getStateFlagsPersistence(),
            initFlags
        );

        volumeProps = SerialPropsContainer.getInstance(dbDriver.getPropsConDriver(), transMgr, srlGen);

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
        String blockDevicePathRef,
        VlmFlags[] flags,
        SerialGenerator serialGen,
        TransactionMgr transMgr,
        boolean createIfNotExists
    )
        throws SQLException
    {
        VolumeData vol = null;

        VolumeDataDatabaseDriver driver = DrbdManage.getVolumeDataDatabaseDriver(resRef, volDfn);
        if (transMgr != null)
        {
            vol = driver.load(transMgr.dbCon, transMgr, serialGen);
        }

        if (vol == null && createIfNotExists)
        {
            long initFlags = StateFlagsBits.getMask(flags);

            vol = new VolumeData(resRef, volDfn, blockDevicePathRef, initFlags, serialGen, transMgr);
            if (transMgr != null)
            {
                driver.create(transMgr.dbCon, vol);
            }
        }

        if (vol != null)
        {
            vol.initialized();
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
        VlmFlagsImpl(
            ObjectProtection objProtRef,
            StateFlagsPersistence persistenceRef,
            long initFlags
        )
        {
            super(
                objProtRef,
                StateFlagsBits.getMask(VlmFlags.values()),
                persistenceRef,
                initFlags
            );
        }
    }

    public String getBlockDevicePath(AccessContext accCtx) throws AccessDeniedException
    {
        resourceRef.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return blockDevicePath;
    }
}
