package com.linbit.linstor.core.objects;

import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public abstract class AbsVolume<RSC extends AbsResource<RSC>>
    extends AbsCoreObj<AbsVolume<RSC>>
    implements LinstorDataObject, ProtectedObject
{

    // Reference to the resource this volume belongs to
    protected final RSC absRsc;

    // Properties container for this volume
    protected final Props props;

    AbsVolume(
        UUID uuid,
        RSC resRef,
        Props propsRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
    {
        super(uuid, transObjFactory, transMgrProviderRef);

        absRsc = resRef;

        props = propsRef;

        transObjs = new ArrayList<>(
            Arrays.asList(
                absRsc,
                props,
                deleted
            )
        );
    }

    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, absRsc.getObjProt(), props);
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

    public abstract long getVolumeSize(AccessContext dbCtxRef) throws AccessDeniedException;
}
