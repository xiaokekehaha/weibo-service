package zx.soft.weibo.sina.user.thread;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.weibo.sina.api.SinaWeiboAPI;

public class UserThread implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(UserThread.class);
	private SinaWeiboAPI api;
	private long now;
	public static final AtomicInteger COUNT = new AtomicInteger(0);

	public UserThread(SinaWeiboAPI api) throws IOException {
		this.api = api;
		now = System.currentTimeMillis();
		logger.info("thread start");

	}

	@Override
	public void run() {

		while (System.currentTimeMillis() < now + 3600000) {
			COUNT.addAndGet(1);
			System.out.println(COUNT.get() + ":" + api.friendshipsFollowers("1642591402", 9, 1, 0));
		}

	}
}
