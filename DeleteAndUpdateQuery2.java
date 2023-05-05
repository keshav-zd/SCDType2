package com.exam1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class DeleteAndUpdateQuery2 {

    public static void main(String arg[])
    {
        Connection conn=null;
        PreparedStatement ps1=null;
        PreparedStatement ps2=null;
        PreparedStatement ps3=null;
        PreparedStatement ps4=null;
        PreparedStatement ps5=null;
        PreparedStatement ps6=null;
        PreparedStatement ps7=null;
        PreparedStatement ps8=null;
        PreparedStatement ps9=null;

        try
        {
            Class.forName("org.postgresql.Driver");
            System.out.println("Open Driver");

            conn= DriverManager.getConnection
                    ("jdbc:postgresql://localhost:5432/keshav","postgres","k@123");
            System.out.println("create connection");

            String s1="copy staging_table from '/home/hp/Documents/myBucket2.csv' delimiter ',' csv header;";
            String s2="\n" +
                    "INSERT INTO dim_table (customer_id, name,address,phone, create_at,update_at)\n" +
                    "        SELECT s.customer_id, s.name, s.address, s.phone, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP\n" +
                    "        FROM staging_table s\n" +
                    "        LEFT JOIN dim_table t\n" +
                    "        ON s.customer_id = t.customer_id\n" +
                    "        WHERE t.customer_id IS NULL;";
            String s3="UPDATE dim_table\n" +
                    "SET  \n" +
                    "is_active=0,\n" +
                    "update_at =CURRENT_TIMESTAMP\n" +
                    "FROM staging_table s\n" +
                    "WHERE dim_table.customer_id=s.customer_id and  (s.name <> dim_table.name OR s.address <>dim_table.address \n" +
                    "\t OR s.phone <> dim_table.phone ) and is_active=1;\n" +
                    "\n";
            String s4="INSERT INTO dim_table (customer_id, name, address,phone, create_at,update_at)\n" +
                    "    SELECT s.customer_id, s.name, s.address, s.phone, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP\n" +
                    "    FROM staging_table s\n" +
                    "    LEFT JOIN dim_table t\n" +
                    "    ON s.customer_id = t.customer_id\n" +
                    "    WHERE t.customer_id IS NOT NULL\n" +
                    "    AND (t.name <> s.name OR t.address <> s.address OR t.phone <> s.phone) \n" +
                    "\tand t.update_at = (SELECT MAX(update_at) FROM dim_table WHERE t.customer_id = dim_table.customer_id);\n";
            String s5="UPDATE dim_table\n" +
                    "SET is_deleted=1,\n" +
                    "\tis_active=0,\n" +
                    "\tupdate_at = now()\n" +
                    "WHERE is_active=1 and NOT EXISTS (SELECT 1 FROM staging_table WHERE dim_table.customer_id = staging_table.customer_id  );\n";
            String s6="\tinsert into final_dim select s.* from staging_table s\n" +
                    "\tleft join final_dim f\n" +
                    "\ton s.customer_id=f.customer_id\n" +
                    "\twhere f.customer_id is null;\n";
            String s7="update final_dim\n" +
                    "set\n" +
                    "update_at=current_timestamp,\n" +
                    "name=s.name,\n" +
                    "address=s.address,\n" +
                    "phone=s.phone\n" +
                    "from staging_table s\n" +
                    "where s.customer_id=final_dim.customer_id\n" +
                    "and (s.name<>final_dim.name or s.address<>final_dim.address or s.phone<>final_dim.phone);\n";
            System.out.println("errore");
            String s8="UPDATE final_dim\n" +
                    "SET is_deleted=1,\n" +
                    "\tis_active=0,\n" +
                    "\tupdate_at = now()\n" +
                    "WHERE NOT EXISTS (SELECT 1 FROM staging_table WHERE final_dim.customer_id = staging_table.customer_id  );\n";
            String s9="TRUNCATE staging_table";

            ps1=conn.prepareStatement(s1);
            ps2=conn.prepareStatement(s2);
            ps3=conn.prepareStatement(s3);
            ps4=conn.prepareStatement(s4);
            ps5=conn.prepareStatement(s5);
            ps6=conn.prepareStatement(s6);
            ps7=conn.prepareStatement(s7);
            ps8=conn.prepareStatement(s8);
            ps9=conn.prepareStatement(s9);

            ps1.executeUpdate();
            ps2.executeUpdate();
            ps3.executeUpdate();
            ps4.executeUpdate();
            ps5.executeUpdate();
            ps6.executeUpdate();
            ps7.executeUpdate();
            ps8.executeUpdate();
            ps9.executeUpdate();
        }
        catch(Exception e)
        {
            System.out.println(e);
        }
        finally {
            try
            {
                if(ps8!=null)
                {
                    ps1.close();
                    ps2.close();
                    ps3.close();
                    ps4.close();
                    ps5.close();
                    ps6.close();
                    ps7.close();
                    ps8.close();
                    ps9.close();


                }
                if(conn!=null)
                {
                    conn.close();
                }
            }
            catch (Exception e)
            {
                System.out.println(e);
            }


        }
    }

}
