package com.hbase.querygenerator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

/*
 * @author: Hisham U
 * @description: For creating a phoenix query from hbase table.
 */

public class HbaseToPhoenixQueryGenerator {
	public static void main(String[] args) {
		Connection hbaseConn = getHbaseConnection(args);
		String[] table = args[0].toUpperCase().split(",");

		for (String tableName : table) {
			try {
				if (isHbaseTableExists(tableName, hbaseConn)) {
					Set<String> columnList = new HashSet<String>();
					Table hbTable = hbaseConn.getTable(TableName.valueOf(tableName));
					Scan scan = new Scan();
					scan.setFilter(new PageFilter(1000));
					ResultScanner results = hbTable.getScanner(scan);
					for (Result result : results) {
						for (Cell cell : result.listCells()) {
							columnList.add("\"" + Bytes.toString(CellUtil.cloneFamily(cell)) + "\"" + "."
									+ Bytes.toString(CellUtil.cloneQualifier(cell)) + " VARCHAR");
						}
					}
					String dataStr = "";
					for (String str : columnList) {
						dataStr = dataStr + "," + str;
					}
					dataStr = dataStr.substring(0, dataStr.length() - 1);
					String phoenixQueryView = "create view " + tableName.toUpperCase() + " (ROW_KEY VARCHAR PRIMARY KEY"
							+ dataStr + ")";
					String phoenixQueryTable = "create table " + tableName.toUpperCase()
							+ " (ROW_KEY VARCHAR PRIMARY KEY" + dataStr + ")";
					System.out.println(phoenixQueryView);
					System.out.println(phoenixQueryTable);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static boolean isHbaseTableExists(String hbaseTableName, Connection connection) {
		boolean isExists = true;
		try {
			Admin hbaseAdmin = connection.getAdmin();
			isExists = hbaseAdmin.tableExists(TableName.valueOf(hbaseTableName));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return isExists;
	}

	public static Connection getHbaseConnection(String[] args) {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", args[1]);
		config.set("hbase.zookeeper.property.clientPort", args[2]);
		config.set("zookeeper.znode.parent", "/hbase-unsecure");
		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(config);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return connection;
	}
}
