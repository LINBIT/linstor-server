package com.linbit.drbdmanage.security;

import com.linbit.ImplementationError;

/**
 * Represents the security context of a thread for access control
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class AccessContext implements Cloneable
{
    public final Identity subjectId;
    public final Role subjectRole;
    public final SecurityType subjectDomain;

    private PrivilegeSet privLimit;
    private PrivilegeSet privEffective;

    AccessContext(Identity subjId, Role subjRole, SecurityType secDomain, PrivilegeSet privLimitRef)
    {
        subjectId = subjId;
        subjectRole = subjRole;
        subjectDomain = secDomain;
        privLimit = privLimitRef;
        privEffective = new PrivilegeSet(privLimit);
    }

    public AccessContext impersonate(
        Identity subjId,
        Role subjRole,
        SecurityType secDomain,
        PrivilegeSet privLimitRef
    )
        throws AccessDeniedException
    {
        privEffective.requirePrivileges(Privilege.PRIV_SYS_ALL);
        AccessContext impCtx = new AccessContext(subjId, subjRole, secDomain, privLimitRef);
        return impCtx;
    }

    @Override
    public AccessContext clone()
    {
        AccessContext clonedCtx = null;
        try
        {
            clonedCtx = (AccessContext) super.clone();
            clonedCtx.privLimit = clonedCtx.privLimit.clone();
            clonedCtx.privEffective = clonedCtx.privEffective.cloneWithLimitPrivs(clonedCtx.privLimit);
        }
        catch (CloneNotSupportedException cloneExc)
        {
            throw new ImplementationError(
                "Cloning failed for class " + AccessContext.class.getName() + "; " +
                "suspected failure to implement the Cloneable interface",
                cloneExc
            );
        }
        return clonedCtx;
    }
}
