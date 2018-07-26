package com.linbit.linstor.event.generator;

import com.linbit.linstor.event.ObjectIdentifier;

public interface ResourceStateGenerator
{
    class UsageState
    {
        Boolean resourceReady = null;
        Boolean inUse = null;

        public Boolean getResourceReady()
        {
            return resourceReady;
        }

        public void setResourceReady(Boolean resourceReady)
        {
            this.resourceReady = resourceReady;
        }

        public Boolean getInUse()
        {
            return inUse;
        }

        public void setInUse(Boolean inUse)
        {
            this.inUse = inUse;
        }
    }

    UsageState generate(ObjectIdentifier objectIdentifier)
        throws Exception;
}
