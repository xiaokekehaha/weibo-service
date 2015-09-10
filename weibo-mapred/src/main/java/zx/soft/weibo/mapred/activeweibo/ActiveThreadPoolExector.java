package zx.soft.weibo.mapred.activeweibo;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.redis.client.cache.Cache;
import zx.soft.redis.client.cache.CacheFactory;
import zx.soft.utils.config.ConfigUtil;
import zx.soft.utils.http.ClientDao;
import zx.soft.utils.http.HttpClientDaoImpl;
import zx.soft.weibo.mapred.source.SourceId;
import zx.soft.weibo.mapred.utils.ClouderaImpalaJdbc;
import zx.soft.weibo.mapred.utils.Constant;
import zx.soft.weibo.mapred.utils.ThreadPoolExecutorUtils;

import com.google.protobuf.ServiceException;

public class ActiveThreadPoolExector {

	static {
		Properties props = ConfigUtil.getProps("super.properties");
		for (String id : props.getProperty("super_user_timeline_active").split(",")) {
			SourceId.addIdUseful(id);
		}
	}
	private static Logger logger = LoggerFactory.getLogger(ActiveThreadPoolExector.class);
	private static long COUNT = 0;

	//从最近爬取的微博表active_user_lastest_weibos中获取最大的微博id,以便在循环爬取活跃用户最新微博时进行since_id更新
	private static String getMaxId() throws SQLException {
		ClouderaImpalaJdbc impala = new ClouderaImpalaJdbc();
		String query = " select max(id) from " + Constant.LASTEST_WEIBO_TABLE;
		ResultSet result = impala.Query(query);
		while (result.next()) {
			String max = result.getString(1);
			return max;
		}
		return null;
	}

	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException,
	ServiceException, InterruptedException, SQLException {
		long count = Long.MAX_VALUE;
		if (args.length >= 1) {
			count = Long.valueOf(args[0]);
			logger.info("Spider count: " + count);
		}
		ClientDao clientDao = new HttpClientDaoImpl();
		int cpuNums = 32;
		ThreadPoolExecutor pool = ThreadPoolExecutorUtils.createExecutor(cpuNums);
		Cache cache = CacheFactory.getInstance();
		logger.info("now , get max weibo id...");
		String sinceId = getMaxId();
		while (count-- > 0 && !pool.isShutdown()) {
			String uid = cache.spop(ActiveThread.ACTIVE_USER);
			logger.info("Retriving uids' count:{}", COUNT++);
			if (uid != null) {
				cache.sadd(ActiveThread.ACTIVE_USER, uid);
				pool.execute(new ActiveThread(sinceId, clientDao, cache));
			} else if (pool.getActiveCount() == 0) {
				logger.info("current pool active count is zero,exit...");
				break;
			}
		}
		pool.shutdown();
		while (!pool.isTerminated()) {
			logger.info("等待子线程全部运行结束,主线程sleep");
			Thread.sleep(60_000);
		}
	}
}
