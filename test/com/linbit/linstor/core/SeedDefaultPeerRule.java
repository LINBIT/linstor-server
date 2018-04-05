package com.linbit.linstor.core;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class SeedDefaultPeerRule implements TestRule
{
    private boolean shouldSeedDefaultPeer = true;

    public boolean shouldSeedDefaultPeer()
    {
        return shouldSeedDefaultPeer;
    }

    @Override
    public Statement apply(final Statement base, final Description description)
    {
        if (description.getAnnotation(DoNotSeedDefaultPeer.class) != null)
        {
            shouldSeedDefaultPeer = false;
        }
        return base;
    }
}
