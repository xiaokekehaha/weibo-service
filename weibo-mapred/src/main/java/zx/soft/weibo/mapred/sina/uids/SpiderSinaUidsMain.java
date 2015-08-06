package zx.soft.weibo.mapred.sina.uids;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.client.HConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.hbase.api.core.HBaseClient;
import zx.soft.hbase.api.core.HConn;
import zx.soft.redis.client.cache.Cache;
import zx.soft.redis.client.cache.CacheFactory;
import zx.soft.utils.config.ConfigUtil;
import zx.soft.utils.http.ClientDao;
import zx.soft.utils.http.HttpClientDaoImpl;
import zx.soft.utils.retry.RetryHandler;
import zx.soft.utils.threads.ApplyThreadPool;
import zx.soft.weibo.mapred.hdfs.HdfsWriter;
import zx.soft.weibo.mapred.hdfs.HdfsWriterSimpleImpl;
import zx.soft.weibo.mapred.source.Polling;
import zx.soft.weibo.mapred.source.SourceId;

import com.google.protobuf.ServiceException;

public class SpiderSinaUidsMain {
	private static long COUNT = 0;

	static {
		Properties props = ConfigUtil.getProps("super.properties");
		for (String id : props.getProperty("super").split(",")) {
			SourceId.addIdUseful(id);
		}
	}

	private static Logger logger = LoggerFactory.getLogger(SpiderSinaUidsMain.class);

	public static void main(String[] args) throws IOException, InterruptedException, ServiceException {
		long count = Long.MAX_VALUE;
		if (args.length >= 1) {
			count = Long.valueOf(args[0]);
			logger.info("Spider count: " + count);
		}

		Cache cache = CacheFactory.getInstance();
		if (args.length >= 2) {
			String seedUid = args[1];
			logger.info("Add seed uid: " + seedUid);
			cache.sadd(Spider.WAIT_USERS_KEY, seedUid);
		}

		// 暂时不开放TSDB统计功能
		//		TsdbReporter reporter = new TsdbReporter(Constant.getTsdbHost(), Constant.getTsdbPort());
		//		reporter.addReport(new GatherQueueReport(cache));

		ClientDao clientDao = new HttpClientDaoImpl();
		SinaRelationshipDao dao = getSinaRelationshipDao(clientDao);
		//创建表
		HBaseClient client = new HBaseClient();
		if (!client.isTableExists(Spider.SINA_USER_BASEINFO)) {
			client.createTable(Spider.SINA_USER_BASEINFO, Spider.USER_BASEINFO_COLUMNFAMILIES);
		}
		if (!client.isTableExists(Spider.SINA_USER_SCORE)) {
			client.createTable(Spider.SINA_USER_SCORE, Spider.USER_SCORE_COLUMNFAMILIES);
		}
		client.close();
		//建立hbase的连接
		HConnection conn = HConn.getHConnection();

		//另起线程循环扫描受到限制的source
		Thread polling = new Thread(new Polling());
		polling.setDaemon(true);
		polling.start();

		final int cpuNum = 64;
		final ThreadPoolExecutor pool = ApplyThreadPool.getThreadPoolExector(cpuNum);
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				pool.shutdown();
			}
		}));

		try (HdfsWriter writer = new HdfsWriterSimpleImpl(Constant.getSinaUserFriendsPath());) {
			while (count-- > 0 && !pool.isShutdown()) {
				String uid = cache.spop(Spider.WAIT_USERS_KEY);
				logger.info("Retriving uids' count:{}", COUNT++);
				if (uid != null) {
					try {
						pool.execute(new Spider(uid, cache, writer, dao, conn));
					} catch (IllegalArgumentException e) {
						logger.warn("illegal argumentException, uid={}", uid);
					} catch (Exception e) {
						logger.error("Thread exception: " + Thread.currentThread().getName(), e);
						break;
					}
				} else if (pool.getActiveCount() == 0) {
					logger.info("WaitUsers queue is empty, exit...");
					break;
				}
			}
			conn.close();
			logger.info("spider count=" + count + ";pool active count=" + pool.getActiveCount());
			pool.shutdown();
			pool.awaitTermination(30, TimeUnit.SECONDS);
		}

		ApplyThreadPool.stop(cpuNum);
	}

	private static SinaRelationshipDao getSinaRelationshipDao(ClientDao clientDao) {
		return (SinaRelationshipDao) Proxy.newProxyInstance(SinaRelationshipDao.class.getClassLoader(),
				new Class[] { SinaRelationshipDao.class }, new RetryHandler<SinaRelationshipDao>(
						new SinaRelationshipDaoImpl(clientDao), 5000, 10) {
					@Override
					protected boolean isRetry(Throwable e) {
						Throwable cause = e.getCause();
						return cause instanceof Exception;
					}
				});
	}

}
