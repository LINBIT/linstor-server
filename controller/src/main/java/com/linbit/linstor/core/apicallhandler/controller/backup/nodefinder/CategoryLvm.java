package com.linbit.linstor.core.apicallhandler.controller.backup.nodefinder;

import com.linbit.linstor.core.identifier.VolumeNumber;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

class CategoryLvm implements Category
{
    private Map<VolumeNumber, Boolean> metadata;

    CategoryLvm(Map<VolumeNumber, Boolean> metadataRef)
    {
        metadata = metadataRef;
    }

    public Map<VolumeNumber, Boolean> getMetadata()
    {
        return metadata;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(metadata);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (!(obj instanceof CategoryLvm))
        {
            return false;
        }
        CategoryLvm other = (CategoryLvm) obj;
        return Objects.equals(metadata, other.metadata);
    }

    @Override
    public int compareTo(Category other)
    {
        int ret;
        if (!(other instanceof CategoryLvm))
        {
            ret = other.getClass().getSimpleName().compareTo(CategoryLvm.class.getSimpleName());
        }
        else
        {
            CategoryLvm otherLvm = (CategoryLvm) other;
            ret = otherLvm.getMetadata().size() - metadata.size();
            if (ret == 0)
            {
                Set<VolumeNumber> keys = new HashSet<>(metadata.keySet());
                keys.addAll(otherLvm.metadata.keySet());
                for (VolumeNumber key : keys)
                {
                    Boolean b1 = metadata.get(key);
                    Boolean b2 = otherLvm.metadata.get(key);
                    if (b1 == null)
                    {
                        ret = -1;
                    }
                    else if (b2 == null)
                    {
                        ret = 1;
                    }
                    else
                    {
                        ret = Boolean.compare(b1, b2);
                    }
                    if (ret != 0)
                    {
                        break;
                    }
                }
            }
        }
        return ret;
    }

}
