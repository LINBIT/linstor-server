package com.linbit.linstor.security;

/**
 * Privileges
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class Privilege
{
    public final long id;
    public final String name;

    // All privileges (aka superuser privileges)
    public static final Privilege PRIV_SYS_ALL = new Privilege(-1L, "PRIV_SYS_ALL");

    // Overrides mandatory access controls
    public static final Privilege PRIV_MAC_OVRD = new Privilege(0x20, "PRIV_MAC_OVRD");

    // Allows changing the object of any owner if the
    // access is allowed by mandatory access controls
    public static final Privilege PRIV_OBJ_OWNER = new Privilege(0x1F, "PRIV_OBJ_OWNER");

    // Overrides object ownership rules to allow modification
    // of any object's access control list
    public static final Privilege PRIV_OBJ_CONTROL = new Privilege(0xF, "PRIV_OBJ_CONTROL");

    // Overrides any object access control list at the
    // CHANGE level of access
    public static final Privilege PRIV_OBJ_CHANGE = new Privilege(0x7, "PRIV_OBJ_CHANGE");

    // Overrides any object access control list at the
    // USE level of access
    public static final Privilege PRIV_OBJ_USE = new Privilege(0x3, "PRIV_OBJ_USE");

    // Overrides any object access control list at the
    // VIEW level of access
    public static final Privilege PRIV_OBJ_VIEW = new Privilege(0x1, "PRIV_OBJ_VIEW");

    public static final Privilege[] PRIVILEGE_LIST =
    {
        PRIV_SYS_ALL,
        PRIV_MAC_OVRD,
        PRIV_OBJ_OWNER,
        PRIV_OBJ_CONTROL,
        PRIV_OBJ_CHANGE,
        PRIV_OBJ_USE,
        PRIV_OBJ_VIEW
    };

    private Privilege(long numId, String nameRef)
    {
        id = numId;
        name = nameRef;
    }
}
