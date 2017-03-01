package com.linbit.drbdmanage.security;

import com.linbit.ImplementationError;

/**
 * Set of privileges
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class PrivilegeSet implements Cloneable
{
    private PrivilegeSet limitPrivs;

    private long privileges;

    PrivilegeSet()
    {
        limitPrivs = null;
        privileges = 0L;
    }

    PrivilegeSet(Privilege... enabledPrivs)
    {
        limitPrivs = null;
        privileges = getPrivMask(enabledPrivs);
    }

    PrivilegeSet(PrivilegeSet limitPrivsRef)
    {
        limitPrivs = limitPrivsRef;
        privileges = 0L;
    }

    /**
     * Checks whether all of the required privileges are enabled
     *
     * @param privList List of required privileges
     * @throws AccessDeniedException If not all required privileges are enabled
     */
    public void requirePrivileges(Privilege... privList)
        throws AccessDeniedException
    {
        long privMask = getPrivMask(privList);
        if (!hasPrivileges(privList))
        {
            throw new AccessDeniedException(
                "Required privileges " + getPrivList(privMask) + " not present"
            );
        }
    }

    /**
     * Indicates whether all of the specified privileges are enabled
     *
     * @param privList List of privileges
     * @return True if all of the specified privileges are enabled, false otherwise
     */
    public boolean hasPrivileges(Privilege... privList)
    {
        long privMask = getPrivMask(privList);
        long limitMask = limitPrivs != null ? limitPrivs.privileges : ~(0L);
        return (privileges & limitMask & privMask) == privMask;
    }

    /**
     * Indicates whether at least one of the specified privileges is enabled
     *
     * @param privList List of privileges
     * @return True if at least one of the specified privileges is enabled, false otherwise
     */
    public boolean hasSomePrivilege(Privilege... privList)
    {
        long privMask = getPrivMask(privList);
        long limitMask = limitPrivs != null ? limitPrivs.privileges : ~(0L);
        return (privileges & limitMask & privMask) != 0L;
    }

    /**
     * Disables the specified privileges
     *
     * @param privList List of privileges to disable
     */
    public void disablePrivileges(Privilege... privList)
    {
        long privMask = ~getPrivMask(privList);
        privileges = privileges & privMask;
    }

    /**
     * Enables the specified privileges
     *
     * @param privList List of privileges to enable
     * @throws AccessDeniedException If one or more of the specified privileges are not present in the
     *     limit privilege set that defines the upper bound of privileges for this privilege set, or
     *     if this privilege set is a limit privilege set and is therefore immutable
     */
    public void enablePrivileges(Privilege... privList)
        throws AccessDeniedException
    {
        if (limitPrivs != null)
        {
            limitPrivs.requirePrivileges(privList);
            long privMask = getPrivMask(privList);
            privileges |= privMask;
        }
        else
        {
            throw new ImplementationError(
                "Attempt to modify the privileges in an immutable privilege set",
                null
            );
        }
    }

    private long getPrivMask(Privilege... privList)
    {
        long privMask = 0L;
        for (Privilege priv : privList)
        {
            privMask |= priv.id;
        }
        return privMask;
    }

    private String getPrivList(long privMask)
    {
        StringBuilder names = new StringBuilder();
        for (Privilege priv : Privilege.PRIVILEGE_LIST)
        {
            if ((privMask & priv.id) == priv.id)
            {
                if (names.length() > 0)
                {
                    names.append(", ");
                }
                names.append(priv.name);
            }
        }
        return names.toString();
    }

    @Override
    public PrivilegeSet clone()
    {
        PrivilegeSet clonedPrivSet = null;
        try
        {
            clonedPrivSet = (PrivilegeSet) super.clone();
        }
        catch (CloneNotSupportedException cloneExc)
        {
            throw new ImplementationError(
                "Cloning failed for class " + AccessContext.class.getName() + "; " +
                "suspected failure to implement the Cloneable interface",
                cloneExc
            );
        }
        return clonedPrivSet;
    }

    public PrivilegeSet cloneWithLimitPrivs(PrivilegeSet limit)
    {
        PrivilegeSet clonedPrivSet = clone();
        clonedPrivSet.limitPrivs = limit;
        clonedPrivSet.privileges &= clonedPrivSet.limitPrivs.privileges;
        return clonedPrivSet;
    }
}
