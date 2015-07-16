package zx.soft.weibo.sina.user.thread;

import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.utils.threads.ApplyThreadPool;
import zx.soft.weibo.sina.api.SinaWeiboAPI;

public class ThreadCore {

	private static Logger logger = LoggerFactory.getLogger(ThreadCore.class);
	private static ThreadPoolExecutor pool;

	public ThreadCore() {
		pool = ApplyThreadPool.getThreadPoolExector(16);
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				pool.shutdown();
				logger.info("pool is shutdown");
			}
		}));
	}

	public void test(SinaWeiboAPI api) {
		if (!pool.isShutdown()) {
			try {
				pool.execute(new UserThread(api));
			} catch (Exception e) {
				logger.error("pool occur error");
				throw new RuntimeException();
			}
		}
	}

	public void close() {
		pool.shutdown();
		try {
			//pool.awaitTermination(20, TimeUnit.SECONDS);
		} catch (Exception e) {
			logger.error(e.getMessage());
			throw new RuntimeException();
		}
	}
}
