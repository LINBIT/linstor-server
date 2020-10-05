package com.linbit.linstor.test.factories;

import com.linbit.InvalidNameException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.StorPoolDefinitionControllerFactory;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.TestAccessContextProvider;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@Singleton
public class StorPoolDefinitionTestFactory
{
    private final StorPoolDefinitionControllerFactory fact;
    private final AtomicInteger nextId = new AtomicInteger();
    private final HashMap<String, StorPoolDefinition> storPoolDfnMap = new HashMap<>();

    private AccessContext dfltAccCtx = TestAccessContextProvider.PUBLIC_CTX;
    private String dfltStorPoolNamePattern = "sp-%d";
    private Supplier<String> dfltStorPoolNameSupplier = () -> String
        .format(dfltStorPoolNamePattern, nextId.incrementAndGet());

    @Inject
    public StorPoolDefinitionTestFactory(
        StorPoolDefinitionControllerFactory factRef
    )
    {
        fact = factRef;
    }

    public StorPoolDefinition get(String storPoolNameRef, boolean createIfNotExists)
        throws AccessDeniedException, DatabaseException, LinStorDataAlreadyExistsException, InvalidNameException
    {
        StorPoolDefinition storPoolDfn = storPoolDfnMap.get(storPoolNameRef.toUpperCase());
        if (storPoolDfn == null && createIfNotExists)
        {
            storPoolDfn = create(storPoolNameRef);
        }
        return storPoolDfn;
    }

    public StorPoolDefinitionTestFactory setDfltAccCtx(AccessContext dfltAccCtxRef)
    {
        dfltAccCtx = dfltAccCtxRef;
        return this;
    }

    public StorPoolDefinitionTestFactory setDfltStorPoolNamePattern(String dfltStorPoolNamePatternRef)
    {
        dfltStorPoolNamePattern = dfltStorPoolNamePatternRef;
        return this;
    }

    public StorPoolDefinition create()
        throws AccessDeniedException, DatabaseException, LinStorDataAlreadyExistsException, InvalidNameException
    {
        return builder(dfltStorPoolNameSupplier.get()).build();
    }

    public StorPoolDefinition create(String storPoolName)
        throws AccessDeniedException, DatabaseException, LinStorDataAlreadyExistsException, InvalidNameException
    {
        return builder(storPoolName).build();
    }

    public StorPoolDefinitionBuilder builder(String storPoolName)
    {
        return new StorPoolDefinitionBuilder(storPoolName);
    }

    public class StorPoolDefinitionBuilder
    {
        private String storPoolName;
        private AccessContext accCtx;

        public StorPoolDefinitionBuilder(String storPoolNameRef)
        {
            storPoolName = storPoolNameRef;
            accCtx = dfltAccCtx;
        }

        public StorPoolDefinitionBuilder setStorPoolName(String storPoolNameRef)
        {
            storPoolName = storPoolNameRef;
            return this;
        }

        public StorPoolDefinitionBuilder setAccCtx(AccessContext accCtxRef)
        {
            accCtx = accCtxRef;
            return this;
        }

        public StorPoolDefinition build()
            throws AccessDeniedException, DatabaseException, LinStorDataAlreadyExistsException, InvalidNameException
        {
            StorPoolDefinition storPoolDfn = fact.create(accCtx, new StorPoolName(storPoolName));
            storPoolDfnMap.put(storPoolName.toUpperCase(), storPoolDfn);
            return storPoolDfn;
        }
    }
}
