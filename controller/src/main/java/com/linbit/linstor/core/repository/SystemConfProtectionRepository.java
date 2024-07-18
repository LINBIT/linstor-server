package com.linbit.linstor.core.repository;

import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyPropsImpl;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

/**
 * Holds the singleton system props protection instance, allowing it to be initialized from the database
 * after dependency injection has been performed.
 */
@Singleton
public class SystemConfProtectionRepository implements SystemConfRepository
{
    private final Props ctrlConf;
    private final Props stltConf;
    private ObjectProtection objectProtection;

    @Inject
    public SystemConfProtectionRepository(
        @Named(LinStor.CONTROLLER_PROPS) Props ctrlConfRef,
        @Named(LinStor.SATELLITE_PROPS) Props stltConfRef
    )
    {
        ctrlConf = ctrlConfRef;
        stltConf = stltConfRef;
    }

    public void setObjectProtection(ObjectProtection objectProtectionRef)
    {
        if (objectProtection != null)
        {
            throw new IllegalStateException("Object protection already set");
        }
        objectProtection = objectProtectionRef;
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkProtSet();
        return objectProtection;
    }

    @Override
    public void requireAccess(AccessContext accCtx, AccessType requested)
        throws AccessDeniedException
    {
        checkProtSet();
        objectProtection.requireAccess(accCtx, requested);
    }

    @Override
    public String setCtrlProp(AccessContext accCtx, String key, String value, String namespace)
        throws InvalidValueException, AccessDeniedException, DatabaseException, InvalidKeyException
    {
        checkProtSet();
        objectProtection.requireAccess(accCtx, AccessType.CHANGE);
        return ctrlConf.setProp(key, value, namespace);
    }

    @Override
    public String setStltProp(AccessContext accCtx, String key, String value)
        throws AccessDeniedException, InvalidValueException, InvalidKeyException, DatabaseException
    {
        checkProtSet();
        objectProtection.requireAccess(accCtx, AccessType.CHANGE);
        return stltConf.setProp(key, value);
    }

    @Override
    public String removeCtrlProp(AccessContext accCtx, String key, String namespace)
        throws AccessDeniedException, InvalidKeyException, DatabaseException
    {
        checkProtSet();
        objectProtection.requireAccess(accCtx, AccessType.CHANGE);
        return ctrlConf.removeProp(key, namespace);
    }

    @Override
    public String removeStltProp(AccessContext accCtx, String key, String namespace)
        throws AccessDeniedException, InvalidKeyException, DatabaseException
    {
        checkProtSet();
        objectProtection.requireAccess(accCtx, AccessType.CHANGE);
        return stltConf.removeProp(key, namespace);
    }

    @Override
    public Props getCtrlConfForView(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkProtSet();
        objectProtection.requireAccess(accCtx, AccessType.VIEW);
        return new ReadOnlyPropsImpl(ctrlConf);
    }

    @Override
    public Props getCtrlConfForChange(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkProtSet();
        objectProtection.requireAccess(accCtx, AccessType.CHANGE);
        return ctrlConf;
    }

    @Override
    public Props getStltConfForView(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkProtSet();
        objectProtection.requireAccess(accCtx, AccessType.VIEW);
        return new ReadOnlyPropsImpl(stltConf);
    }

    private void checkProtSet()
    {
        if (objectProtection == null)
        {
            throw new IllegalStateException("Object protection not yet set");
        }
    }
}
