package zx.soft.weibo.mapred.sina.weibo;

import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.redis.client.cache.Cache;
import zx.soft.redis.client.cache.CacheFactory;
import zx.soft.utils.config.ConfigUtil;
import zx.soft.utils.http.ClientDao;
import zx.soft.utils.http.HttpClientDaoImpl;
import zx.soft.weibo.mapred.sina.uids.Spider;
import zx.soft.weibo.mapred.source.SourceId;
import zx.soft.weibo.mapred.utils.ThreadPoolExecutorUtils;

public class HistoryWeiboThreadPoolExector {

	static {
		Properties props = ConfigUtil.getProps("super.properties");
		for (String id : props.getProperty("super_user_timeline").split(",")) {
			SourceId.addIdUseful(id);
		}
	}

	private static Logger logger = LoggerFactory.getLogger(HistoryWeiboThreadPoolExector.class);
	private static long COUNT = 0;

	public static void main(String[] args) {
		long count = Long.MAX_VALUE;
		if (args.length >= 1) {
			count = Long.valueOf(args[0]);
			logger.info("history Spider count: " + count);
		}
		int cpuNums = 4;
		ThreadPoolExecutor pool = ThreadPoolExecutorUtils.createExecutor(cpuNums);
		Cache cache = CacheFactory.getInstance();
		ClientDao client = new HttpClientDaoImpl();

		while (count-- > 0 && !pool.isShutdown()) {
			String uid = cache.spop(Spider.PROCESSED_USERS_KEY);
			logger.info("Retriving uids' count:{}", COUNT++);
			if (uid != null) {
				pool.execute(new HistoryWeiboThread(uid, client, cache));
			} else if (pool.getActiveCount() == 0) {
				logger.info("processedUsers queue is empty, exit...");
				break;
			}
		}

		logger.info("spider count=" + count + ";pool active count=" + pool.getActiveCount());
		pool.shutdown();
	}
}
