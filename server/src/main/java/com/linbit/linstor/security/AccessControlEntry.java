package com.linbit.linstor.security;

import java.util.Objects;

/**
 * Access control entry for access control lists
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class AccessControlEntry implements Comparable<AccessControlEntry>
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

    @Override
    public int compareTo(AccessControlEntry oRef)
    {
        int cmp = objPath.compareTo(oRef.objPath);
        if (cmp == 0)
        {
            cmp = subjectRole.compareTo(oRef.subjectRole);
        }
        return cmp;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(objPath, subjectRole);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (!(obj instanceof AccessControlEntry))
        {
            return false;
        }
        AccessControlEntry other = (AccessControlEntry) obj;
        return Objects.equals(objPath, other.objPath) && Objects.equals(subjectRole, other.subjectRole);
    }
}
