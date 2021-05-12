package com.linbit.linstor.storage;

import java.util.Objects;

public class ProcCryptoEntry
{

    public enum CryptoType
    {
        SKCIPHER("skcipher"),
        CIPHER("cipher"),
        SHASH("shash"),
        AHASH("ahash"),
        AEAD("aead"),
        KPP("kpp"),
        AKCIPHER("akcipher"),
        COMPRESSION("compression"),
        SCOMP("scomp"),
        RNG("rng"),
        UNKNOWN("");

        private final String name;
        CryptoType(String nameRef)
        {
            this.name = nameRef;
        }

        public String getName()
        {
            return name;
        }

        public static CryptoType fromString(String type)
        {
            CryptoType result = UNKNOWN;
            for (CryptoType ct : CryptoType.values())
            {
                if (type.equals(ct.getName()))
                {
                    result = ct;
                    break;
                }
            }
            return result;
        }

        @Override
        public String toString()
        {
            return name;
        }
    }

    private final String name;
    private final String driver;
    private final CryptoType type;
    private final int priority;

    public ProcCryptoEntry(
        String nameRef,
        String driverRef,
        CryptoType typeRef,
        int priorityRef
    )
    {
        name = nameRef;
        driver = driverRef;
        type = typeRef;
        priority = priorityRef;
    }

    public String getName()
    {
        return name;
    }

    public String getDriver()
    {
        return driver;
    }

    public CryptoType getType()
    {
        return type;
    }

    public int getPriority()
    {
        return priority;
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof ProcCryptoEntry && (driver.equals(((ProcCryptoEntry) obj).driver));
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(driver);
    }

    @Override
    public String toString()
    {
        return "ProcCryptoEntry{" +
            "name='" + name + '\'' +
            ", driver='" + driver + '\'' +
            ", type=" + type +
            ", priority=" + priority +
            '}';
    }
}
