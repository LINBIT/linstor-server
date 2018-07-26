package com.linbit.linstor.security;

/**
 * Access control entry for access control lists
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class AccessControlEntry
{
    public final Role subjectRole;
    public final AccessType access;

    AccessControlEntry(Role subjRole, AccessType acc)
    {
        subjectRole = subjRole;
        access = acc;
    }
}
