package com.linbit.linstor.core;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collections;

import com.linbit.TransactionMgr;
import com.linbit.linstor.dbdrivers.DerbyDriver;
import com.linbit.linstor.dbdrivers.derby.DerbyConstants;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.Privilege;

/**
 * This class is only for debugging and should never go public.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
class CtrlResetHandler
{
    public static void reset(Controller controller)
    {
        TransactionMgr transMgr = null;
        try
        {
            transMgr = new TransactionMgr(controller.dbConnPool.getConnection());

            controller.reconfigurationLock.writeLock().lock();

            ArrayList<String> tableNames = new ArrayList<>();
            for (Field field : DerbyConstants.class.getDeclaredFields())
            {
                if (field.getName().startsWith("TBL_"))
                {
                    tableNames.add((String) field.get(null));
                }
            }
            Collections.reverse(tableNames);
            for (String tableName : tableNames)
            {
                try (PreparedStatement stmt = transMgr.dbCon.prepareStatement("DELETE FROM " + tableName))
                {
                    stmt.executeUpdate();
                }
            }

            Field sysCtxField = Controller.class.getDeclaredField("sysCtx");
            boolean accessible = sysCtxField.isAccessible();
            sysCtxField.setAccessible(true);
            AccessContext ctrlSysCtx = (AccessContext) sysCtxField.get(controller);
            sysCtxField.setAccessible(accessible);

            AccessContext privCtx = ctrlSysCtx.clone();
            privCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);

            transMgr.commit();

            controller.nodesMap.clear();
            controller.rscDfnMap.clear();
            controller.storPoolDfnMap.clear();

            LinStor.persistenceDbDriver = new DerbyDriver(
                privCtx,
                controller.getErrorReporter(),
                controller.nodesMap,
                controller.rscDfnMap,
                controller.storPoolDfnMap
            );

            String sql = new String(Files.readAllBytes(Paths.get("sql-src", "drbd-init-derby.sql")));
            StringBuilder commandBuilder = new StringBuilder();
            String[] split = sql.split("\n");
            boolean insert = false;
            for (String line : split)
            {
                String trimmedLine = line.trim();
                if (trimmedLine.startsWith("INSERT") || insert)
                {
                    commandBuilder.append(trimmedLine);
                    if (trimmedLine.endsWith(";"))
                    {
                        insert = false;
                        commandBuilder.setLength(commandBuilder.length()-1); // cut the last ';'
                        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(commandBuilder.toString()))
                        {
                            stmt.executeUpdate();
                        }
                        commandBuilder.setLength(0);
                    }
                    else
                    {
                        insert = true;
                        commandBuilder.append("\n");
                    }
                }
            }
            transMgr.commit();

            controller.shutdown(ctrlSysCtx, false);
            controller.initialize(controller.getErrorReporter());
        }
        catch (Exception exc)
        {
            controller.getErrorReporter().reportError(exc);
        }
        finally
        {
            controller.dbConnPool.returnConnection(transMgr);
            controller.reconfigurationLock.writeLock().unlock();
        }
    }
}
