package com.linbit.linstor.core.objects;

import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

/**
 * Abstract base class for volumes belonging to a resource.
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public abstract class AbsVolume<RSC extends AbsResource<RSC>>
    extends AbsCoreObj<AbsVolume<RSC>>
    implements LinstorDataObject
{

    // Reference to the resource this volume belongs to
    protected final RSC absRsc;

    AbsVolume(
        UUID uuid,
        RSC resRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
    {
        super(uuid, transObjFactory, transMgrProviderRef);

        absRsc = resRef;

        transObjs = new ArrayList<>(
            Arrays.asList(
                absRsc,
                deleted
            )
        );
    }

    public RSC getAbsResource()
    {
        checkDeleted();
        return absRsc;
    }

    public ResourceDefinition getResourceDefinition()
    {
        checkDeleted();
        return absRsc.getResourceDefinition();
    }

    public abstract VolumeDefinition getVolumeDefinition();

    public abstract VolumeNumber getVolumeNumber();

    public abstract long getVolumeSize();
}
