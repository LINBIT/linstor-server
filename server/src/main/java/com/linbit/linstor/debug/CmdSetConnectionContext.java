package com.linbit.linstor.debug;

import com.linbit.AutoIndent;
import com.linbit.InvalidNameException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.Identity;
import com.linbit.linstor.security.IdentityName;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.security.Role;
import com.linbit.linstor.security.RoleName;
import com.linbit.linstor.security.SecTypeName;
import com.linbit.linstor.security.SecurityType;
import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;
import javax.inject.Inject;

public class CmdSetConnectionContext extends BaseDebugCmd
{
    private static final String PRM_CONN_ID = "CONNID";
    private static final String PRM_ID      = "IDENTITY";
    private static final String PRM_ROLE    = "ROLE";
    private static final String PRM_DOMAIN  = "DOMAIN";

    private static final String CTX_CREATION_FAILED = "Creation of a new access context failed";

    private static final Map<String, String> PARAMETER_DESCRIPTIONS = new TreeMap<>();
    static
    {
        PARAMETER_DESCRIPTIONS.put(
            PRM_CONN_ID,
            "Connection ID of the connection to modify"
        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_ID,
            "Name of the identity for the new access context"
        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_ROLE,
            "Name of the role for the new access context"
        );
        PARAMETER_DESCRIPTIONS.put(
            PRM_DOMAIN,
            "Name of the security domain for the new access context"
        );
    }

    private final CoreModule.PeerMap peerMap;

    @Inject
    public CmdSetConnectionContext(
        CoreModule.PeerMap peerMapRef
    )
    {
        super(
            new String[]
            {
                "SetConCtx"
            },
            "Set the access context of a peer connection",
            "Sets the access context associated with a connected peer",
            PARAMETER_DESCRIPTIONS,
            null
        );

        peerMap = peerMapRef;
    }

    @Override
    public void execute(
        PrintStream debugOut,
        PrintStream debugErr,
        AccessContext accCtx,
        Map<String, String> parameters
    )
        throws Exception
    {
        try
        {
            String connId = parameters.get(PRM_CONN_ID);

            IdentityName idName = null;
            {
                String idPrm = parameters.get(PRM_ID);
                if (idPrm != null)
                {
                    try
                    {
                        idName = new IdentityName(idPrm);
                    }
                    catch (InvalidNameException nameExc)
                    {
                        throw new LinStorException(
                            "The name '" + idPrm + "' is not a valid identity name",
                            "The specified identity name is not valid",
                            "An invalid name was specified for the " + PRM_ID + " parameter",
                            "Reenter the command using a valid identity name",
                            nameExc.getMessage()
                        );
                    }
                }
            }

            RoleName rlName = null;
            {
                String rlPrm = parameters.get(PRM_ROLE);
                if (rlPrm != null)
                {
                    try
                    {
                        rlName = new RoleName(rlPrm);
                    }
                    catch (InvalidNameException nameExc)
                    {
                        throw new LinStorException(
                            "The name '" + rlPrm + "' is not a valid role name",
                            "The specified role name is not valid",
                            "An invalid name was specified for the " + PRM_ROLE + " parameter",
                            "Reenter the command using a valid role name",
                            nameExc.getMessage()
                        );
                    }
                }
            }

            SecTypeName dmnName = null;
            {
                String dmnPrm = parameters.get(PRM_DOMAIN);
                if (dmnPrm != null)
                {
                    try
                    {
                        dmnName = new SecTypeName(dmnPrm);
                    }
                    catch (InvalidNameException nameExc)
                    {
                        throw new LinStorException(
                            "The name '" + dmnPrm + "' is not a valid security domain name",
                            "The specified security domain name is not valid",
                            "An invalid name was specified for the " + PRM_DOMAIN + " parameter",
                            "Reenter the command using a valid security domain name",
                            nameExc.getMessage()
                        );
                    }
                }
            }

            if (idName == null && rlName == null && dmnName == null)
            {
                throw new LinStorException(
                    "Nothing to do, because none of the parameters " + PRM_ID + ", " + PRM_ROLE + " or " +
                        PRM_DOMAIN + " were set",
                    "Incomplete command line, nothing to do",
                    "No modification for the access context was specified",
                    "At least one of the parameters " + PRM_ID + ", " + PRM_ROLE + " or " + PRM_DOMAIN +
                    " must be set\nfor this command to have any effect",
                    null
                );
            }

            if (connId != null)
            {
                Peer client = peerMap.get(connId);
                if (client != null)
                {
                    AccessContext curCtx = client.getAccessContext();

                    Identity idObj = (idName == null ? curCtx.subjectId : Identity.get(idName));
                    Role rlObj = (rlName == null ? curCtx.subjectRole : Role.get(rlName));
                    SecurityType dmnObj = (dmnName == null ? curCtx.subjectDomain : SecurityType.get(dmnName));

                    if (idObj == null)
                    {
                        throw new LinStorException(
                            "The specified identity '" + idName.displayValue + "' does not exist",
                            CTX_CREATION_FAILED,
                            "The specified identity does not exist",
                            "Reenter the command using the name of an existing identity",
                            "The specified identity name was '" + idName.displayValue + "'"
                        );
                    }
                    if (rlObj == null)
                    {
                        throw new LinStorException(
                            "The specified role '" + rlName.displayValue + "' does not exist",
                            CTX_CREATION_FAILED,
                            "The specified role does not exist",
                            "Reenter the command using the name of an existing role",
                            "The specified role name was '" + rlName.displayValue + "'"
                        );
                    }
                    if (dmnObj == null)
                    {
                        throw new LinStorException(
                            "The specified security domain '" + dmnName.displayValue + "' does not exist",
                            CTX_CREATION_FAILED,
                            "The specified security domain does not exist",
                            "Reenter the command using the name of an existing security domain",
                            "The specified security domain name was '" + dmnName.displayValue + "'"
                        );
                    }

                    AccessContext privCtx = accCtx.clone();
                    privCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);

                    AccessContext newCtx = privCtx.impersonate(idObj, rlObj, dmnObj);
                    client.setAccessContext(privCtx, newCtx);

                    debugOut.println("New access conntext for connection to " + client + ":");
                    AutoIndent.printWithIndent(
                        debugOut, AutoIndent.DEFAULT_INDENTATION,
                        String.format(
                            "Identity:        %s\n" +
                            "Role:            %s\n" +
                            "Security domain: %s\n",
                            idObj.name.displayValue, rlObj.name.displayValue, dmnObj.name.displayValue
                        )
                    );
                }
                else
                {
                    throw new LinStorException(
                        "No connection was found for connection id '" + connId + "'",
                        "The specified connection could not be found",
                        "No connection that matches the connection ID specified for the " +
                        PRM_CONN_ID + " parameter was found",
                        "Specify the connection ID of an active connection.",
                        "The specified connection ID was '" + connId + "'"
                    );
                }
            }
            else
            {
                printMissingParamError(debugErr, PRM_CONN_ID);
            }
        }
        catch (LinStorException lsExc)
        {
            printLsException(debugErr, lsExc);
        }
    }
}
