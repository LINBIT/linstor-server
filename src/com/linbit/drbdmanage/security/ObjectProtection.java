package com.linbit.drbdmanage.security;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.drbdmanage.InvalidNameException;

/**
 * Security protection for drbdmanageNG object
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class ObjectProtection
{
    public static final String DEFAULT_SECTYPE_NAME = "default";

    // Identity that created the object
    //
    // The creator's identity may change if the
    // account that was used to create the object
    // is deleted
    private Identity objectCreator;

    // Role that has owner rights on the object
    private Role objectOwner;

    // Access control list for the object
    private final AccessControlList objectAcl;

    // Security type for the object
    private final SecurityType objectType;

    private ObjectProtection(Identity creator, Role owner)
    {
        objectCreator = creator;
        objectOwner = owner;
        objectAcl = new AccessControlList();
        try
        {
            objectType = new SecurityType(new SecTypeName(DEFAULT_SECTYPE_NAME));
        }
        catch (InvalidNameException invNameExc)
        {
            throw new ImplementationError(
                "Default security type name is invalid",
                null
            );
        }
    }

    /**
     * Creates an ObjectProtection instance for an newly created object
     *
     * @param accCtx The object creator's access context
     */
    public ObjectProtection(AccessContext accCtx)
    {
        ErrorCheck.ctorNotNull(ObjectProtection.class, AccessContext.class, accCtx);

        objectCreator = accCtx.subjectId;
        objectOwner = accCtx.subjectRole;
        objectAcl = new AccessControlList();
        objectType = accCtx.subjectDomain;
    }

    /**
     * Check whether a subject can be granted the requested level of access
     * to the object protected by this instance
     *
     * @param context The security context of the subject requesting access
     * @param requested The type of access requested by the subject
     * @throws AccessDeniedException If access is denied
     */
    public final void requireAccess(AccessContext context, AccessType requested)
        throws AccessDeniedException
    {
        objectType.requireAccess(context, requested);
        objectAcl.requireAccess(context, requested);
    }
}
