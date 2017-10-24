package com.linbit.drbdmanage.security;

/**
 * Security database table and field names
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class SecurityDbFields
{
    static final String TBL_IDENTITIES      = "SEC_IDENTITIES";
    static final String TBL_ROLES           = "SEC_ROLES";
    static final String TBL_TYPES           = "SEC_TYPES";
    static final String TBL_TYPE_RULES      = "SEC_TYPE_RULES";
    static final String TBL_ID_ROLE_MAP     = "SEC_ID_ROLE_MAP";
    static final String TBL_DFLT_ROLES      = "SEC_DFLT_ROLES";
    static final String TBL_OBJ_PROT        = "SEC_OBJECT_PROTECTION";
    static final String TBL_ACL_MAP         = "SEC_ACL_MAP";
    static final String TBL_SEC_CFG         = "SEC_CONFIGURATION";

    static final String VW_IDENTITIES_LOAD  = "SEC_IDENTITIES_LOAD";
    static final String VW_ROLES_LOAD       = "SEC_ROLES_LOAD";
    static final String VW_TYPES_LOAD       = "SEC_TYPES_LOAD";
    static final String VW_TYPE_RULES_LOAD  = "SEC_TYPE_RULES_LOAD";

    static final String IDENTITY_NAME       = "IDENTITY_NAME";
    static final String IDENTITY_DSP_NAME   = "IDENTITY_DSP_NAME";
    static final String PASS_SALT           = "PASS_SALT";
    static final String PASS_HASH           = "PASS_HASH";
    static final String ID_ENABLED          = "ID_ENABLED";
    static final String ID_LOCKED           = "ID_LOCKED";

    static final String ROLE_NAME           = "ROLE_NAME";
    static final String ROLE_DSP_NAME       = "ROLE_DSP_NAME";
    static final String ROLE_ENABLED        = "ROLE_ENABLED";
    static final String ROLE_PRIVILEGES     = "ROLE_PRIVILEGES";

    static final String TYPE_NAME           = "TYPE_NAME";
    static final String TYPE_DSP_NAME       = "TYPE_DSP_NAME";
    static final String DOMAIN_NAME         = "DOMAIN_NAME";
    static final String TYPE_ENABLED        = "TYPE_ENABLED";
    static final String ACCESS_TYPE         = "ACCESS_TYPE";
    static final String OBJECT_PATH         = "OBJECT_PATH";
    static final String CRT_IDENTITY_NAME   = "CREATOR_IDENTITY_NAME";
    static final String OWNER_ROLE_NAME     = "OWNER_ROLE_NAME";
    static final String SEC_TYPE_NAME       = "SECURITY_TYPE_NAME";

    static final String CONF_KEY            = "ENTRY_KEY";
    static final String CONF_DSP_KEY        = "ENTRY_DSP_KEY";
    static final String CONF_VALUE          = "ENTRY_VALUE";

    static final String KEY_SEC_LEVEL       = "SECURITYLEVEL";
    static final String KEY_DSP_SEC_LEVEL   = "SecurityLevel";

    private SecurityDbFields()
    {
    }
}
