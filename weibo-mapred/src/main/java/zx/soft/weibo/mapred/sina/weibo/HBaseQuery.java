package zx.soft.weibo.mapred.sina.weibo;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.hbase.client.HConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.hbase.api.core.HBaseClient;
import zx.soft.hbase.api.core.HBaseTable;
import zx.soft.hbase.api.core.HConn;

import com.google.protobuf.ServiceException;

public class HBaseQuery {
	private static Logger logger = LoggerFactory.getLogger(HBaseQuery.class);
	private HConnection conn;
	public static final String DATA_COUNT = "data_count";
	public static final String DATA_COUNT_CF = "count";

	public HBaseQuery() throws IOException, ServiceException {
		//创建表
		HBaseClient client = new HBaseClient();
		if (!client.isTableExists(DATA_COUNT)) {
			client.createTable(DATA_COUNT, "count");
		}
		client.close();
		conn = HConn.getHConnection();
	}

	public void calHistoryWeiboCount(long minStamp, long maxStamp, String tableName, String cf, String q) {
		HBaseTable weibo = null;
		HBaseTable count = null;
		try {
			weibo = new HBaseTable(conn, tableName);
			count = new HBaseTable(conn, DATA_COUNT);
			count.put(String.valueOf(minStamp), DATA_COUNT_CF, String.valueOf(maxStamp),
					String.valueOf(weibo.getRowsCount(minStamp, maxStamp, cf, q)));
			count.execute();
			logger.info("add calculate data to data_count");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			weibo.close();
			count.close();
		}
	}

	public static void main(String[] args) throws InterruptedException {
		long minStamp = 0;
		long maxStamp = 0;
		long now = 0;
		try {
			if (args.length > 0) {
				minStamp = Long.valueOf(args[0]);
			} else {
				minStamp = (new Date().getTime() / (3600_000 * 24)) * 3600_000 * 24;
			}
			HBaseQuery query = new HBaseQuery();
			while (true) {
				for (int j = 1; j < 25; j++) {
					now = System.currentTimeMillis();
					maxStamp = minStamp + j * 3600_000;
					if (now < maxStamp) {
						Thread.sleep(maxStamp - now);
					}
					query.calHistoryWeiboCount(minStamp, maxStamp, "history_weibo", "history", "id");
				}
				minStamp = maxStamp;
			}

		} catch (IOException | ServiceException e) {
			logger.error(e.getMessage());
		}

	}
}
