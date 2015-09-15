package zx.soft.weibo.mapred.sina.uids;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.redis.client.cache.Cache;
import zx.soft.redis.client.cache.CacheFactory;
import zx.soft.utils.config.ConfigUtil;
import zx.soft.utils.http.ClientDao;
import zx.soft.utils.http.HttpClientDaoImpl;
import zx.soft.utils.retry.RetryHandler;
import zx.soft.weibo.mapred.hdfs.HdfsWriter;
import zx.soft.weibo.mapred.hdfs.HdfsWriterSimpleImpl;
import zx.soft.weibo.mapred.source.SourceId;
import zx.soft.weibo.mapred.utils.ThreadPoolExecutorUtils;

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
		ClientDao clientDao = new HttpClientDaoImpl();
		SinaRelationshipDao dao = getSinaRelationshipDao(clientDao);

		final int cpuNum = 4;
		final ThreadPoolExecutor pool = ThreadPoolExecutorUtils.createExecutor(cpuNum);

		try (HdfsWriter writer = new HdfsWriterSimpleImpl(Constant.getSinaUserFriendsPath());) {
			while (count-- > 0 && !pool.isShutdown()) {
				String uid = cache.spop(Spider.WAIT_USERS_KEY);
				logger.info("Retriving uids' count:{}", COUNT++);
				if (uid != null) {
					try {
						pool.execute(new Spider(uid, cache, writer, dao, clientDao));
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
			logger.info("spider count=" + count + ";pool active count=" + pool.getActiveCount());
			pool.shutdown();
		}

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
