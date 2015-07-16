package zx.soft.weibo.mapred.user;

import zx.soft.redis.client.cache.Cache;
import zx.soft.redis.client.cache.CacheFactory;
import zx.soft.weibo.mapred.sina.uids.Spider;

public class SinaUserInfo {

	private static Cache cache = CacheFactory.getInstance();

	public static String getNextId() {
		String id = cache.spop(Spider.WAIT_USERS_KEY);
		System.out.println(id);
		cache.sadd(Spider.WAIT_USERS_KEY, id);
		return id;
	}

}
