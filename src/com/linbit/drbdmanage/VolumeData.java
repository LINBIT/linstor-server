package com.linbit.drbdmanage;

import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDatabaseDriver;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsAccess;
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
public class VolumeData implements Volume
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

    private VolumeDatabaseDriver dbDriver;

    private VolumeData(Resource resRef, VolumeDefinition volDfn)
    {
        objId = UUID.randomUUID();
        resourceRef = resRef;
        resourceDfn = resRef.getDefinition();
        volumeDfn = volDfn;

        dbDriver = DrbdManage.getVolumeDatabaseDriver(this);

        flags = new VlmFlagsImpl(
            resRef.getObjProt(),
            dbDriver.getStateFlagsPersistence()
        );
    }

    // TODO: create static VolumeData.create(...)
    // TODO: create static VolumeData.load(...)

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

    @Override
    public void setConnection(TransactionMgr transMgr) throws ImplementationError
    {
        transMgr.register(this);
        dbDriver.setConnection(transMgr.dbCon);
    }

    @Override
    public void commit()
    {
        resourceRef.commit();
        volumeDfn.commit();
        volumeProps.commit();
        flags.commit();
    }

    @Override
    public void rollback()
    {
        resourceRef.rollback();
        volumeDfn.rollback();
        volumeProps.rollback();
        flags.rollback();
    }

    @Override
    public boolean isDirty()
    {
        return resourceRef.isDirty() ||
            resourceDfn.isDirty() ||
            volumeDfn.isDirty() ||
            volumeProps.isDirty() ||
            flags.isDirty();
    }
}
