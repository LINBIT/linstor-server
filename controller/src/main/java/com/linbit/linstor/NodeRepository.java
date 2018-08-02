package com.linbit.linstor;

import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;

/**
 * Provides access to nodes with automatic security checks.
 */
public interface NodeRepository
{
    ObjectProtection getObjProt();

    void requireAccess(AccessContext accCtx, AccessType requested)
        throws AccessDeniedException;

    NodeData get(AccessContext accCtx, NodeName nodeName)
        throws AccessDeniedException;

    void put(AccessContext accCtx, NodeName nodeName, Node node)
        throws AccessDeniedException;

    void remove(AccessContext accCtx, NodeName nodeName)
        throws AccessDeniedException;

    CoreModule.NodesMap getMapForView(AccessContext accCtx)
        throws AccessDeniedException;
}
