package com.test;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Mysql {

	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost:3306/ad_f33ba6ada231ff9?useSSL=false&user=root&password=root";

	// Database credentials
	static final String USER = "root";
	static final String PASS = "root";

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("hello world");

		java.sql.Connection conn = null;
		java.sql.Statement stmt = null;
		try {
			// STEP 2: Register JDBC driver
			Class.forName("com.mysql.jdbc.Driver");

			// STEP 3: Open a connection
			System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);

			// STEP 4: Execute a query
			System.out.println("Creating statement...");
			stmt = conn.createStatement();
			String sql;
			sql = "SELECT H_RCN,     HR_MPN,     H_SERIAL,     HR_MSN,     NOUN,     DESCRIPTION,     H_ACN,     FLEET,     FLEET_MODEL_CD,     H_POS,     HI_DTE,     HI_STA,     HI_DEPT,     HR_DTE,     HR_STA,     HR_DEPT,     HR_REASON,     HR_TSI,     HR_CSI,     HR_ATA,     HR_D_NBR,     HS_STA,     HS_DEPT,     HS_REPAIR_DTE,     HS_REPAIR_TYPE,     HS_REPAIR_ODR_NBR,     master FROM test;";
			ResultSet rs = stmt.executeQuery(sql);
			int a = 1;
			// STEP 5: Extract data from result set
			while (rs.next()) {
				// Retrieve by column name
				// int id = rs.getInt("componentid");
				// int age = rs.getInt("age");
				// String first = rs.getString("first");
				// String last = rs.getString("last");

				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
				String H_RCN = rs.getString("H_RCN");
				String HR_MPN = rs.getString("HR_MPN");
				String H_SERIAL = rs.getString("H_SERIAL");
				String HR_MSN = rs.getString("HR_MSN");
				String NOUN = rs.getString("NOUN");
				String DESCRIPTION = rs.getString("DESCRIPTION");
				String H_ACN = rs.getString("H_ACN");
				String FLEET = rs.getString("FLEET");
				String FLEET_MODEL_CD = rs.getString("FLEET_MODEL_CD");
				String H_POS = rs.getString("H_POS");
				// String HI_DTE = rs.getString("HI_DTE");
				String HI_DTE = null;
				String HI_DTE_S = rs.getString("HI_DTE");
				if (HI_DTE_S != null) {

					HI_DTE = rs.getString("HI_DTE");
				}
				String HI_STA = rs.getString("HI_STA");
				String HI_DEPT = rs.getString("HI_DEPT");
				// String HR_DTE = rs.getString("HR_DTE");
				String HR_DTE = null;
				String HR_DTE_S = rs.getString("HR_DTE");
				if (HR_DTE_S != null) {

					HR_DTE = rs.getString("HR_DTE");
				}
				String HR_STA = rs.getString("HR_STA");
				String HR_DEPT = rs.getString("HR_DEPT");
				String HR_REASON = rs.getString("HR_REASON");
				String HR_TSI = rs.getString("HR_TSI");
				String HR_CSI = rs.getString("HR_CSI");
				String HR_ATA = rs.getString("HR_ATA");
				String HR_D_NBR = rs.getString("HR_D_NBR");
				String HS_STA = rs.getString("HS_STA");
				String HS_DEPT = rs.getString("HS_DEPT");
				// String HS_REPAIR_DTE = rs.getString("HS_REPAIR_DTE");
				String HS_REPAIR_DTE = null;
				String HS_REPAIR_DTE_S = rs.getString("HS_REPAIR_DTE");
				if (HS_REPAIR_DTE_S != null) {

					HS_REPAIR_DTE = rs.getString("HS_REPAIR_DTE");
				}
				String HS_REPAIR_TYPE = rs.getString("HS_REPAIR_TYPE");
				String HS_REPAIR_ODR_NBR = rs.getString("HS_REPAIR_ODR_NBR");
				String master = rs.getString("master");

				Statement master_check = conn.createStatement();
				String ms_ck = "select count(master) from component where master = '" + master + "'";
				
				ResultSet r1 = master_check.executeQuery(ms_ck);
				r1.next();
				int rowcount = r1.getInt(1);


				if (rowcount > 0) {

					Statement st1 = conn.createStatement();
					
					String in1 = "select componentid from component where master = '" + master + "'";

					ResultSet r2 = st1.executeQuery(in1);

					int comp_id = 0;

					while (r2.next()) {

						comp_id = r2.getInt("componentid");

					}

					

					if (HI_DTE != null) {
						// System.out.println("inserting install data");
						
						Statement history_check = conn.createStatement();
						String hs_ck = "select count(historyid) from component_history where componentid = '" + comp_id + "'";
						// System.out.println(hs_ck);
						ResultSet rh = history_check.executeQuery(hs_ck);
						rh.next();
						int hs_rowcount = rh.getInt(1);
						

						if(hs_rowcount>0){
						
						
						
						Statement st2 = conn.createStatement();

						String in2 = "select historyid from component_history where componentid = '" + comp_id
								+ "' and status = 'Removed' order by historyid desc limit 1 ";

						ResultSet rs2 = st2.executeQuery(in2);

						while (rs2.next()) {
							// System.out.println("Executing stmt");
							Statement st3 = conn.createStatement();

							String in3 = "INSERT INTO component_history(componentid, from_date, status, maint_stn, dept,tail_no) VALUES('"
									+ comp_id + "','" + HI_DTE + "','Installed Unit','" + HI_STA + "','" + HI_DEPT + "','"
									+ H_ACN + "')";
							in3 = in3.replaceAll("\"null\"", "null");
							in3 = in3.replaceAll("\"Null\"", "null");
							in3 = in3.replaceAll("\"NULL\"", "null");

							st3.executeUpdate(in3);

							Statement st4 = conn.createStatement();

							String in4 = "UPDATE component_history set to_date = '" + HI_DTE + "' where historyid = '"
									+ rs2.getInt("historyid") + "'";
							
							in4 = in4.replaceAll("\"null\"", "null");
							in4 = in4.replaceAll("\"Null\"", "null");
							in4 = in4.replaceAll("\"NULL\"", "null");

							st4.executeUpdate(in4);

							Statement st4_s = conn.createStatement();

							String in4_s = "UPDATE component set status = 'Installed Unit', status_updated_date = '" + HI_DTE
									+ "' where componentid = '" + comp_id + "'";
							
							in4_s = in4_s.replaceAll("\"null\"", "null");
							in4_s = in4_s.replaceAll("\"Null\"", "null");
							in4_s = in4_s.replaceAll("\"NULL\"", "null");

							st4_s.executeUpdate(in4_s);

						}

						rs2.close();

					}else{
						// System.out.println("Executing stmt");
						Statement st3 = conn.createStatement();

						String in3 = "INSERT INTO component_history(componentid, from_date, status, maint_stn, dept,tail_no) VALUES('"
								+ comp_id + "','" + HI_DTE + "','Installed Unit','" + HI_STA + "','" + HI_DEPT + "','"
								+ H_ACN + "')";
						
						in3 = in3.replaceAll("\"null\"", "null");
						in3 = in3.replaceAll("\"Null\"", "null");
						in3 = in3.replaceAll("\"NULL\"", "null");

						st3.executeUpdate(in3);
						
						Statement st4_s = conn.createStatement();

						String in4_s = "UPDATE component set status = 'Installed Unit', status_updated_date = '" + HI_DTE
								+ "' where componentid = '" + comp_id + "'";
						
						in4_s = in4_s.replaceAll("\"null\"", "null");
						in4_s = in4_s.replaceAll("\"Null\"", "null");
						in4_s = in4_s.replaceAll("\"NULL\"", "null");

						st4_s.executeUpdate(in4_s);


					}
						
					}

					if (HR_DTE != null) {
						// System.out.println("inserting removal data");
						Statement st5 = conn.createStatement();

						String in5 = "select historyid from component_history where componentid = '" + comp_id
								+ "' and status = 'Installed Unit' order by historyid desc limit 1 ";

						ResultSet rs5 = st5.executeQuery(in5);

						while (rs5.next()) {
							// System.out.println("Executing stmt");
							Statement st6 = conn.createStatement();

							String in6 = "INSERT INTO component_history(componentid, from_date, status,to_date, maint_stn, dept,status_reason,discrepency_no,position_component_removal,tail_no) VALUES('"
									+ comp_id + "','" + HR_DTE + "','Removed','" + HR_DTE + "','" + HR_STA + "','" + HR_DEPT + "','"
									+ HR_REASON + "','" + HR_D_NBR + "','" + H_POS + "','" + H_ACN + "')";
							
							in6 = in6.replaceAll("\"null\"", "null");
							in6 = in6.replaceAll("\"Null\"", "null");
							in6 = in6.replaceAll("\"NULL\"", "null");

							st6.executeUpdate(in6);

							Statement st7 = conn.createStatement();

							String in7 = "UPDATE component_history set to_date = '" + HR_DTE + "' where historyid = '"
									+ rs5.getInt("historyid") + "'";
							
							in7 = in7.replaceAll("\"null\"", "null");
							in7 = in7.replaceAll("\"Null\"", "null");
							in7 = in7.replaceAll("\"NULL\"", "null");

							st7.executeUpdate(in7);

							Statement st7_s = conn.createStatement();

							String in7_s = "UPDATE component set status = 'Inactive', status_updated_date = '" + HR_DTE
									+ "' where componentid = '" + comp_id + "'";
							
							in7_s = in7_s.replaceAll("\"null\"", "null");
							in7_s = in7_s.replaceAll("\"Null\"", "null");
							in7_s = in7_s.replaceAll("\"NULL\"", "null");

							st7_s.executeUpdate(in7_s);

						}

						rs5.close();

					}

					if (HS_REPAIR_DTE != null) {
						// System.out.println("inserting repair data");
						Statement st9 = conn.createStatement();
						// System.out.println("Executing stmt");
						String in9 = "INSERT INTO component_history(componentid, from_date, status,to_date, maint_stn, dept,status_reason) VALUES('"
								+ comp_id + "','" + HS_REPAIR_DTE + "','Repair " + HS_REPAIR_TYPE + "','"
								+ HS_REPAIR_DTE + "','" + HS_STA + "','" + HS_DEPT + "','" + HS_REPAIR_ODR_NBR + "')";
						
						in9 = in9.replaceAll("\"null\"", "null");
						in9 = in9.replaceAll("\"Null\"", "null");
						in9 = in9.replaceAll("\"NULL\"", "null");

						st9.executeUpdate(in9);

					}
				} else {

					Statement st10 = conn.createStatement();

					String in10 = "INSERT INTO component(ata_System_no,company_part_no,fleet_no,mfg_part_no,cmpy_serial_no,subfleet_no,tail_no,mnfg_serial_no,classification,description,cycles,master,time_since_install) VALUES('"
							+ HR_ATA + "','" + H_RCN + "','" + FLEET + "','" + HR_MPN + "','" + H_SERIAL + "','"
							+ FLEET_MODEL_CD + "','" + H_ACN + "','" + HR_MSN + "','" + NOUN + "','" + DESCRIPTION
							+ "','" + HR_CSI + "','" + master + "','" + HR_TSI + "')";
					
					in10 = in10.replaceAll("\"null\"", "null");
					in10 = in10.replaceAll("\"Null\"", "null");
					in10 = in10.replaceAll("\"NULL\"", "null");
						// System.out.println(in10);
					st10.executeUpdate(in10);
				}

				// Statement st = conn.createStatement();
				// String sq = "";
				// st.executeUpdate(sq);
				//
				// Statement st1 = conn.createStatement();
				// String sq1 = "";
				// st.executeUpdate(sq1);

				// Display values
				
				System.out.println(a);
				
				a++;
				// // System.out.print(", Age: " + age);
				// // System.out.print(", First: " + first);
				// // System.out.println(", Last: " + last);
			}
			// STEP 6: Clean-up environment
			rs.close();
			stmt.close();
			conn.close();
		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
		} finally {
			// finally block used to close resources
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			} // nothing we can do
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			} // end finally try
		} // end try
		System.out.println("Goodbye!");

	}
}
