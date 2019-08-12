package com.linbit.linstor.security.data;

public class TypeEnforcementRule
{
    private final String domainName;
    private final String typeName;
    private final String accessType;

    public TypeEnforcementRule(
        final String domainNameRef,
        final String typeNameRef,
        final String accessTypeRef
    )
    {
        domainName = domainNameRef;
        typeName = typeNameRef;
        accessType = accessTypeRef;
    }

    public String getDomainName()
    {
        return domainName;
    }

    public String getTypeName()
    {
        return typeName;
    }

    public String getAccessType()
    {
        return accessType;
    }
}
