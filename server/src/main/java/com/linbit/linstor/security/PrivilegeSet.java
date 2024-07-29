package com.linbit.linstor.security;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Set of privileges
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class PrivilegeSet implements Cloneable
{
    private @Nullable PrivilegeSet limitPrivs;

    // Privileges bit field
    // Assignment is guaranteed to be atomic by means of declaring the long volatile
    private volatile long privileges;

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

    PrivilegeSet(long enabledPrivs)
    {
        limitPrivs = null;
        privileges = enabledPrivs;
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

        SecurityLevel globalSecLevel = SecurityLevel.get();
        switch (globalSecLevel)
        {
            case NO_SECURITY:
                break;
            case RBAC:
                // fall-through
            case MAC:
                if (!hasPrivileges(privList))
                {
                    throw new AccessDeniedException(
                        "Required privileges " + getPrivList(privMask) + " not present"
                    );
                }
                break;
            default:
                throw new ImplementationError(
                    "Missing case label for enum constant " + globalSecLevel.name(),
                    null
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
        synchronized (this)
        {
            privileges &= privMask;
            if (limitPrivs != null)
            {
                privileges &= limitPrivs.privileges;
            }
        }
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
            synchronized (this)
            {
                limitPrivs.requirePrivileges(privList);
                long privMask = getPrivMask(privList);
                privileges = (privileges & limitPrivs.privileges) | privMask;
            }
        }
        else
        {
            throw new ImplementationError(
                "Attempt to enable privileges in the limit privilege set",
                null
            );
        }
    }

    public List<Privilege> getEnabledPrivileges()
    {
        List<Privilege> privList = new LinkedList<>();
        long enabledPrivs = limitPrivs == null ? privileges : privileges & limitPrivs.privileges;
        for (Privilege priv : Privilege.PRIVILEGE_LIST)
        {
            if ((enabledPrivs & priv.id) == priv.id)
            {
                privList.add(priv);
            }
        }
        return privList;
    }

    public Privilege[] toArray()
    {
        List<Privilege> privList = getEnabledPrivileges();
        Privilege[] privArray = new Privilege[privList.size()];
        int index = 0;
        for (Privilege priv : privList)
        {
            privArray[index] = priv;
            ++index;
        }
        return privArray;
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

    /**
     * Returns the AccessType granted by enabled privileges for MAC security components.
     * The returned AccessType may be used to override MAC access restrictions.
     *
     * @return MAC AccessType granted by enabled privileges
     */
    public @Nullable AccessType toMacAccess()
    {
        AccessType result = null;
        long limitMask = limitPrivs != null ? limitPrivs.privileges : ~(0L);
        long privs = privileges & limitMask;
        if ((privs & Privilege.PRIV_MAC_OVRD.id) == Privilege.PRIV_MAC_OVRD.id)
        {
            result = AccessType.CONTROL;
        }
        return result;
    }

    /**
     * Returns the AccessType granted by enabled privileges for RBAC security components
     * The returned AccessType may be used to override RBAC access restrictions.
     *
     * @return RBAC AccessType granted by enabled privileges
     */
    public @Nullable AccessType toRbacAccess()
    {
        AccessType result = null;
        long limitMask = limitPrivs != null ? limitPrivs.privileges : ~(0L);
        long privs = privileges & limitMask;
        if ((privs & Privilege.PRIV_OBJ_CONTROL.id) == Privilege.PRIV_OBJ_CONTROL.id ||
            (privs & Privilege.PRIV_OBJ_OWNER.id) == Privilege.PRIV_OBJ_OWNER.id)
        {
            result = AccessType.CONTROL;
        }
        else
        if ((privs & Privilege.PRIV_OBJ_CHANGE.id) == Privilege.PRIV_OBJ_CHANGE.id)
        {
            result = AccessType.CHANGE;
        }
        else
        if ((privs & Privilege.PRIV_OBJ_USE.id) == Privilege.PRIV_OBJ_USE.id)
        {
            result = AccessType.USE;
        }
        else
        if ((privs & Privilege.PRIV_OBJ_VIEW.id) == Privilege.PRIV_OBJ_VIEW.id)
        {
            result = AccessType.VIEW;
        }
        return result;
    }

    static Set<Privilege> privMaskToPrivSet(long privMask)
    {
        Set<Privilege> privSet = new TreeSet<>();
        for (Privilege priv : Privilege.PRIVILEGE_LIST)
        {
            if ((privMask & priv.id) == priv.id)
            {
                privSet.add(priv);
            }
        }
        return privSet;
    }
}
