package com.linbit.linstor.security.pojo;

import com.linbit.linstor.annotation.Nullable;
public class TypeEnforcementRulePojo implements Comparable<TypeEnforcementRulePojo>
{
    private final String domainName;
    private final String typeName;
    private final short accessType;

    public TypeEnforcementRulePojo(
        final String domainNameRef,
        final String typeNameRef,
        final short accessTypeRef
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

    public short getAccessType()
    {
        return accessType;
    }

    @Override
    public int compareTo(TypeEnforcementRulePojo other)
    {
        int cmp = compareNullable(domainName, other.domainName);
        if (cmp == 0)
        {
            cmp = compareNullable(typeName, other.typeName);
            if (cmp == 0)
            {
                cmp = Short.compare(accessType, other.accessType);
            }
        }
        return cmp;
    }

    private int compareNullable(@Nullable String str1, @Nullable String str2)
    {
        int cmp;
        if (str1 == null && str2 == null)
        {
            cmp = 0;
        }
        else
        {
            if (str1 == null)
            {
                cmp = -1;
            }
            else
            {
                if (str2 == null)
                {
                    cmp = 1;
                }
                else
                {
                    cmp = str1.compareTo(str2);
                }
            }
        }
        return cmp;
    }
}
