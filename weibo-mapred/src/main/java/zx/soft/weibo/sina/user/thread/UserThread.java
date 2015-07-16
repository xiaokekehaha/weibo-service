package zx.soft.weibo.sina.user.thread;

import java.io.IOException;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.hbase.api.core.HBaseTable;
import zx.soft.weibo.mapred.sina.uids.Spider;
import zx.soft.weibo.mapred.user.SinaUserInfo;
import zx.soft.weibo.sina.api.SinaWeiboAPI;
import zx.soft.weibo.sina.domain.SinaDomain;

public class UserThread implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(UserThread.class);
	private SinaWeiboAPI api;
	private String cookie;
	private HBaseTable table;
	private long now;

	public UserThread(SinaWeiboAPI api, String cookie, HBaseTable table) throws IOException {
		this.api = api;
		this.cookie = cookie;
		this.table = table;
		now = System.currentTimeMillis();
		logger.info("thread start");
	}

	@Override
	public void run() {

		while (System.currentTimeMillis() < now + 3600000) {
			SinaDomain sinaDomain = api.usersShow(SinaUserInfo.getNextId(), cookie);
			Collection<String> fileds = sinaDomain.getFieldNames();
			for (String field : fileds) {
				try {
					table.put(sinaDomain.get("uid").toString(), Spider.USER_BASEINFO_COLUMNFAMILIES, field, sinaDomain
							.get(field).toString());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

}
