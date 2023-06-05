package com.linbit.linstor.security;

/**
 * Access control entry for access control lists
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class AccessControlEntry
{
    public final String objPath;
    public final Role subjectRole;
    public final AccessType access;

    AccessControlEntry(String objPathRef, Role subjRoleRef, AccessType accRef)
    {
        objPath = objPathRef;
        subjectRole = subjRoleRef;
        access = accRef;
    }
}
