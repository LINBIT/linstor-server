package com.linbit.linstor.core;

import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.DummySecurityInitializer;
import com.linbit.linstor.security.TestAccessContextProvider;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class SeedDefaultPeerRule implements TestRule
{
    protected static final AccessContext SYS_CTX = DummySecurityInitializer.getSystemAccessContext();
    protected static final AccessContext PUBLIC_CTX = DummySecurityInitializer.getPublicAccessContext();

    protected static final AccessContext ALICE_ACC_CTX;
    protected static final AccessContext BOB_ACC_CTX;
    static
    {
        ALICE_ACC_CTX = TestAccessContextProvider.ALICE_ACC_CTX;
        BOB_ACC_CTX = TestAccessContextProvider.BOB_ACC_CTX;
    }

    private AccessContext defaultPeerAccessContext = ALICE_ACC_CTX;

    public boolean shouldSeedDefaultPeer()
    {
        return defaultPeerAccessContext != null;
    }

    public void setDefaultPeerAccessContext(AccessContext defaultPeerAccessContextRef)
    {
        defaultPeerAccessContext = defaultPeerAccessContextRef;
    }

    public AccessContext getDefaultPeerAccessContext()
    {
        return defaultPeerAccessContext;
    }

    @Override
    public Statement apply(final Statement base, final Description description)
    {
        if (description.getAnnotation(DoNotSeedDefaultPeer.class) != null)
        {
            defaultPeerAccessContext = null;
        }
        return base;
    }
}
