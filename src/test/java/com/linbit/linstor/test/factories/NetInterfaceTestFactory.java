package com.linbit.linstor.test.factories;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.core.objects.NetInterfaceFactory;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.TestAccessContextProvider;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@Singleton
public class NetInterfaceTestFactory
{
    private final NetInterfaceFactory netIfFact;
    private final NodeTestFactory nodeFact;
    private final HashMap<Pair<String, String>, NetInterface> netIfMap = new HashMap<>();

    private final AtomicInteger nextIp = new AtomicInteger(1);
    private final AtomicInteger nextPort = new AtomicInteger(19000);

    public AccessContext dfltAccCtx = TestAccessContextProvider.PUBLIC_CTX;
    public Supplier<LsIpAddress> dfltAddrSupplier =
        () ->
        {
            LsIpAddress ipObj = null;
            try
            {
                do
                {
                    final int ipOffset = nextIp.getAndIncrement();
                    final int fourth = ipOffset % 256;
                    if (fourth != 0 && fourth != 255)
                    {
                        final int third = ipOffset / 256;
                        ipObj = new LsIpAddress("10.0." + third + "." + fourth);
                    }
                }
                while (ipObj == null);
            }
            catch (InvalidIpAddressException exc)
            {
                throw new ImplementationError(exc);
            }
            return ipObj;
        };

    public Supplier<TcpPortNumber> dfltPortSupplier =
        () ->
        {
            try
            {
                return new TcpPortNumber(nextPort.getAndIncrement());
            }
            catch (ValueOutOfRangeException exc)
            {
                throw new ImplementationError(exc);
            }
        };

    public EncryptionType dfltEncrType = EncryptionType.PLAIN;

    @Inject
    public NetInterfaceTestFactory(
        NetInterfaceFactory netIfFactRef,
        NodeTestFactory nodeFactRef
    )
    {
        netIfFact = netIfFactRef;
        nodeFact = nodeFactRef;
    }

    public NetInterface get(String nodeName, String netIfName, boolean createIfNotExists)
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, InvalidNameException
    {
        NetInterface netIf = netIfMap.get(new Pair<>(nodeName.toUpperCase(), netIfName.toUpperCase()));
        if (netIf == null && createIfNotExists)
        {
            netIf = create(nodeName, netIfName);
        }
        return netIf;
    }

    public NetInterfaceTestFactory setDfltAccCtx(AccessContext dfltAccCtxRef)
    {
        dfltAccCtx = dfltAccCtxRef;
        return this;
    }

    public NetInterfaceTestFactory setDfltAddrSupplier(Supplier<LsIpAddress> dfltAddrSupplierRef)
    {
        dfltAddrSupplier = dfltAddrSupplierRef;
        return this;
    }

    public NetInterfaceTestFactory setDfltPortSupplier(Supplier<TcpPortNumber> dfltPortSupplierRef)
    {
        dfltPortSupplier = dfltPortSupplierRef;
        return this;
    }

    public NetInterfaceTestFactory setDfltEncrType(EncryptionType dfltEncrTypeRef)
    {
        dfltEncrType = dfltEncrTypeRef;
        return this;
    }

    public NetInterface create(Node node, String netIfName)
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, InvalidNameException
    {
        return builder(node.getName().displayValue, netIfName).build();
    }

    public NetInterface create(String nodeName, String netIfName)
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, InvalidNameException
    {
        return builder(nodeName, netIfName).build();
    }

    public NetInterfaceBuilder builder(Node node, String netIfName)
    {
        return new NetInterfaceBuilder(node.getName().displayValue, netIfName);
    }

    public NetInterfaceBuilder builder(String nodeName, String netIfName)
    {
        return new NetInterfaceBuilder(nodeName, netIfName);
    }

    public class NetInterfaceBuilder
    {
        private AccessContext accCtx;
        private String nodeName;
        private String netIfName;
        private LsIpAddress addr;
        private TcpPortNumber port;
        private EncryptionType encrType;

        public NetInterfaceBuilder(String nodeNameRef, String netIfNameRef)
        {
            nodeName = nodeNameRef;
            netIfName = netIfNameRef;

            accCtx = dfltAccCtx;
            addr = dfltAddrSupplier.get();
            port = dfltPortSupplier.get();
            encrType = dfltEncrType;
        }

        public NetInterfaceBuilder setAccCtx(AccessContext accCtxRef)
        {
            accCtx = accCtxRef;
            return this;
        }

        public NetInterfaceBuilder setNodeName(String nodeNameRef)
        {
            nodeName = nodeNameRef;
            return this;
        }

        public NetInterfaceBuilder setNetIfName(String netIfNameRef)
        {
            netIfName = netIfNameRef;
            return this;
        }

        public NetInterfaceBuilder setAddr(LsIpAddress addrRef)
        {
            addr = addrRef;
            return this;
        }

        public NetInterfaceBuilder setPort(TcpPortNumber portRef)
        {
            port = portRef;
            return this;
        }

        public NetInterfaceBuilder setEncrType(EncryptionType encrTypeRef)
        {
            encrType = encrTypeRef;
            return this;
        }

        public NetInterface build()
            throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException, InvalidNameException
        {
            NetInterface netIf = netIfFact.create(accCtx, nodeFact.get(nodeName, true), new NetInterfaceName(netIfName), addr, port, encrType);
            netIfMap.put(new Pair<>(nodeName.toUpperCase(), netIfName.toUpperCase()), netIf);
            return netIf;
        }
    }

}
