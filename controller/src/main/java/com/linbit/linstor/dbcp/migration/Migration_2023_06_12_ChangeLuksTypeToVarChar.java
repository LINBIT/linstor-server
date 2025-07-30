package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.utils.Base64;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("checkstyle:typename")
@Migration(
    version = "2023.06.12.10.00",
    description = "Change LuksVlm's encrypted type to varchar"
)
public class Migration_2023_06_12_ChangeLuksTypeToVarChar extends LinstorMigration
{
    private static final String TBL = "LAYER_LUKS_VOLUMES";

    private static final String CLM_RSC_ID = "LAYER_RESOURCE_ID";
    private static final String CLM_VLM_NR = "VLM_NR";
    private static final String CLM_ENCRYPTED_PASSWORD = "ENCRYPTED_PASSWORD";

    @Override
    public void migrate(Connection connection, DbProduct dbProduct) throws Exception
    {
        List<LuksVlm> vlms = new ArrayList<>();
        try (
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(
                "SELECT " + CLM_RSC_ID + ", " + CLM_VLM_NR + ", " + CLM_ENCRYPTED_PASSWORD + " FROM " + TBL
            );
        )
        {
            while (rs.next())
            {
                vlms.add(
                    new LuksVlm(
                        rs.getInt(CLM_RSC_ID),
                        rs.getInt(CLM_VLM_NR),
                        rs.getBytes(CLM_ENCRYPTED_PASSWORD)
                    )
                );
            }
        }
        MigrationUtils.dropColumn(dbProduct, TBL, CLM_ENCRYPTED_PASSWORD);
        MigrationUtils.addColumn(
            dbProduct,
            TBL,
            CLM_ENCRYPTED_PASSWORD,
            "VARCHAR2",
            true, // for now
            null,
            CLM_VLM_NR
        );

        try (
            PreparedStatement ps = connection.prepareStatement(
                "UPDATE " + TBL +
                " SET " + CLM_ENCRYPTED_PASSWORD + " = ? " +
                "WHERE " + CLM_RSC_ID + " = ? AND " +
                           CLM_VLM_NR + " = ?"
            );
        )
        {
            for (LuksVlm luksVlm : vlms)
            {
                ps.setString(1, Base64.encode(luksVlm.encryptedPw));
                ps.setInt(2, luksVlm.rscId);
                ps.setInt(3, luksVlm.vlmNr);

                ps.execute();
            }
        }
        MigrationUtils.addColumnConstraintNotNull(dbProduct, TBL, CLM_ENCRYPTED_PASSWORD, "VARCHAR2");
    }

    private class LuksVlm
    {
        int rscId;
        int vlmNr;
        byte[] encryptedPw;

        LuksVlm(int rscIdRef, int vlmNrRef, byte[] encryptedPwRef)
        {
            super();
            rscId = rscIdRef;
            vlmNr = vlmNrRef;
            encryptedPw = encryptedPwRef;
        }
    }
}
