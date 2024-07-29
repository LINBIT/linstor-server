package com.linbit.linstor.core.repository;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ProtectedObject;

/**
 * Provides access to system-wide props with automatic security checks.
 * The controller (ctrl) props are those which only the controller uses.
 * The satellite (stlt) props are those which are transfered to the satellites.
 */
public interface SystemConfRepository extends ProtectedObject
{
    void requireAccess(AccessContext accCtx, AccessType requested)
        throws AccessDeniedException;

    @Nullable
    String setCtrlProp(AccessContext accCtx, String key, String value, @Nullable String namespace)
        throws InvalidValueException, AccessDeniedException, DatabaseException, InvalidKeyException;

    @Nullable
    String setStltProp(AccessContext accCtx, String key, String value)
        throws AccessDeniedException, InvalidValueException, InvalidKeyException, DatabaseException;

    @Nullable
    String removeCtrlProp(AccessContext accCtx, String key, @Nullable String namespace)
        throws AccessDeniedException, InvalidKeyException, DatabaseException;

    @Nullable
    String removeStltProp(AccessContext accCtx, String key, @Nullable String namespace)
        throws AccessDeniedException, InvalidKeyException, DatabaseException;

    ReadOnlyProps getCtrlConfForView(AccessContext accCtx)
        throws AccessDeniedException;

    Props getCtrlConfForChange(AccessContext accCtx)
        throws AccessDeniedException;

    ReadOnlyProps getStltConfForView(AccessContext accCtx)
        throws AccessDeniedException;
}
