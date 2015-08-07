package zx.soft.weibo.mapred.activeweibo;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HConnection;

import zx.soft.hbase.api.core.HBaseClient;
import zx.soft.hbase.api.core.HConn;
import zx.soft.utils.config.ConfigUtil;
import zx.soft.utils.http.ClientDao;
import zx.soft.utils.http.HttpClientDaoImpl;
import zx.soft.utils.threads.ApplyThreadPool;
import zx.soft.weibo.mapred.source.SourceId;

import com.google.protobuf.ServiceException;

public class ActiveThreadPoolExector {
	static {
		Properties props = ConfigUtil.getProps("super.properties");
		for (String id : props.getProperty("super_user_timeline_active").split(",")) {
			SourceId.addIdUseful(id);
		}
	}

	public static String sinceId = "3872946600794464";

	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException,
	ServiceException, InterruptedException {
		ClientDao clientDao = new HttpClientDaoImpl();
		//创建表,并建立hbase的连接
		HBaseClient client = new HBaseClient();
		if (!client.isTableExists(ActiveThread.ACTIVE_USERS_LASTEST_WEIBOS)) {
			client.createTable(ActiveThread.ACTIVE_USERS_LASTEST_WEIBOS,
					ActiveThread.ACTIVE_USERS_LASTEST_WEIBOS_COLUMNFAMILY);
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
		while (!pool.isShutdown()) {
			for (int i = 0; i < 10000; i++) {
				pool.execute(new ActiveThread(i * 1000, conn, sinceId));
			}
			while (!pool.awaitTermination(1, TimeUnit.HOURS)) {
				//等待子线程全部运行结束
			}
			//更新since id,循环获取活跃用户微博,传入url查询ACTIVE_USERS_LASTEST_WEIBOS表中最大微博id
			sinceId = clientDao.doGet("");
		}

		pool.shutdown();
		pool.awaitTermination(30, TimeUnit.SECONDS);
		ApplyThreadPool.stop(cpuNums);
	}

}
