package zx.soft.weibo.mapred.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import zx.soft.utils.config.ConfigUtil;

public class ClouderaImpalaJdbc {

	private static Connection conn;

	public ClouderaImpalaJdbc() {

		Properties props = ConfigUtil.getProps("hive-connection.properties");
		try {
			Class.forName(props.getProperty("jdbc.driver.class.name"));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		try {
			conn = DriverManager.getConnection(props.getProperty("connection.url"));
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public ResultSet Query(String sqlStatement) {
		ResultSet result = null;
		try {
			Statement statement = conn.createStatement();
			result = statement.executeQuery(sqlStatement);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		return result;
	}

	public Connection getConnection() {
		return conn;
	}

	public void closeConnection() {
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
