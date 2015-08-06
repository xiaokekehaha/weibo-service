package zx.soft.weibo.mapred.sina.weibo;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HConnection;

import zx.soft.hbase.api.core.HBaseClient;
import zx.soft.hbase.api.core.HConn;
import zx.soft.utils.config.ConfigUtil;
import zx.soft.utils.threads.ApplyThreadPool;
import zx.soft.weibo.mapred.source.SourceId;

import com.google.protobuf.ServiceException;

public class ThreadPoolExector {

	static {
		Properties props = ConfigUtil.getProps("super.properties");
		for (String id : props.getProperty("super_user_timeline").split(",")) {
			SourceId.addIdUseful(id);
		}
	}

	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException,
	ServiceException, InterruptedException {
		//创建表,并建立hbase的连接
		HBaseClient client = new HBaseClient();
		if (!client.isTableExists(HistoryWeiboThread.History_Weibo)) {
			client.createTable(HistoryWeiboThread.History_Weibo, HistoryWeiboThread.History_Weibo_ColumnFamily);
		}
		client.close();
		HConnection conn = HConn.getHConnection();
		int cpuNums = 64;
		final ThreadPoolExecutor pool = ApplyThreadPool.getThreadPoolExector(cpuNums);
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				pool.shutdown();
			}
		}));
		List<List<String>> lists = UidsList.getSubList();
		for (int i = 0; i < lists.size(); i++) {
			pool.execute(new HistoryWeiboThread(lists.get(i), conn));
		}
		pool.shutdown();
		pool.awaitTermination(30, TimeUnit.SECONDS);
		ApplyThreadPool.stop(cpuNums);
	}

}
