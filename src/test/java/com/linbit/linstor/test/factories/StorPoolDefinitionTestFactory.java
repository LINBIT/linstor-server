package com.linbit.linstor.test.factories;

import com.linbit.InvalidNameException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.StorPoolDefinitionControllerFactory;
import com.linbit.linstor.core.objects.StorPoolDefinitionDbDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDatabaseDriver;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@Singleton
public class StorPoolDefinitionTestFactory
{
    private final StorPoolDefinitionControllerFactory fact;
    private final StorPoolDefinitionDatabaseDriver spdDbDriver;
    private final AtomicInteger nextId = new AtomicInteger();
    private final HashMap<String, StorPoolDefinition> storPoolDfnMap = new HashMap<>();

    private String dfltStorPoolNamePattern = "sp-%d";
    private Supplier<String> dfltStorPoolNameSupplier = () -> String
        .format(dfltStorPoolNamePattern, nextId.incrementAndGet());

    @Inject
    public StorPoolDefinitionTestFactory(
        StorPoolDefinitionControllerFactory factRef,
        StorPoolDefinitionDatabaseDriver spdDbDriverRef
    )
    {
        fact = factRef;
        spdDbDriver = spdDbDriverRef;
    }

    /*
     * This method has to be called as a workaround. We cannot create "dfltDisklessStorPool" as it already exists in DB
     * But we cannot load it in the constructor (during guice injection) as we are not in a scope at the at time.
     */
    public void initDfltDisklessStorPool() throws DatabaseException
    {
        StorPoolDefinition dfltDisklessStorPoolDfn = ((StorPoolDefinitionDbDriver) spdDbDriver).loadAll(null)
            .keySet()
            .stream()
            .filter(spd -> spd.getName().displayValue.equalsIgnoreCase(LinStor.DISKLESS_STOR_POOL_NAME))
            .findFirst()
            .get();

        storPoolDfnMap.put(LinStor.DISKLESS_STOR_POOL_NAME.toUpperCase(), dfltDisklessStorPoolDfn);
    }

    public StorPoolDefinition get(String storPoolNameRef, boolean createIfNotExists)
        throws DatabaseException, LinStorDataAlreadyExistsException, InvalidNameException
    {
        StorPoolDefinition storPoolDfn = storPoolDfnMap.get(storPoolNameRef.toUpperCase());
        if (storPoolDfn == null && createIfNotExists)
        {
            storPoolDfn = create(storPoolNameRef);
        }
        return storPoolDfn;
    }

    public StorPoolDefinitionTestFactory setDfltStorPoolNamePattern(String dfltStorPoolNamePatternRef)
    {
        dfltStorPoolNamePattern = dfltStorPoolNamePatternRef;
        return this;
    }

    public StorPoolDefinition create()
        throws DatabaseException, LinStorDataAlreadyExistsException, InvalidNameException
    {
        return builder(dfltStorPoolNameSupplier.get()).build();
    }

    public StorPoolDefinition create(String storPoolName)
        throws DatabaseException, LinStorDataAlreadyExistsException, InvalidNameException
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

        public StorPoolDefinitionBuilder(String storPoolNameRef)
        {
            storPoolName = storPoolNameRef;
        }

        public StorPoolDefinitionBuilder setStorPoolName(String storPoolNameRef)
        {
            storPoolName = storPoolNameRef;
            return this;
        }

        public StorPoolDefinition build()
            throws DatabaseException, LinStorDataAlreadyExistsException, InvalidNameException
        {
            StorPoolDefinition storPoolDfn = fact.create(new StorPoolName(storPoolName));
            storPoolDfnMap.put(storPoolName.toUpperCase(), storPoolDfn);
            return storPoolDfn;
        }
    }
}
