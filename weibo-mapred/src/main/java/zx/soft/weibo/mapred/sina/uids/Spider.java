package zx.soft.weibo.mapred.sina.uids;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.redis.client.cache.Cache;
import zx.soft.weibo.mapred.hdfs.HdfsWriter;

public class Spider implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(Spider.class);

	private final SinaRelationshipDao relationshipDao;

	private final HdfsWriter writer;

	private final String uid;

	private final Cache cache;

	public static final String CLOSE_USERS_KEY = "sent:sina:closeUsers";

	public static final String PROCESSED_USERS_KEY = "sent:sina:processedUsers";

	public static final String WAIT_USERS_KEY = "sent:sina:waitUsers";

	/**
	 * 如果members不在key2和key3所关联的set中，则保存到key1所关联的set中
	 */
	public static final String SADD_IF_NOT_EXIST_Others_script = "local count = 0\n" //
			+ "for i, uid in ipairs(ARGV) do\n" //
			+ "    if redis.call('sismember', KEYS[2], uid) == 0 and redis.call('sismember', KEYS[3], uid) == 0 then\n" //
			+ "        redis.call('sadd', KEYS[1], uid)\n" //
			+ "        count = count + 1\n" //
			+ "    end\n" //
			+ "end\n" //
			+ "return count";

	public Spider(String uid, final Cache cache, HdfsWriter writer, SinaRelationshipDao relationshipDao) {
		if (StringUtils.isEmpty(uid)) {
			logger.error("Uid is empty!");
			throw new IllegalArgumentException("uid is empty");
		}
		this.uid = uid;
		this.cache = cache;
		this.writer = writer;
		this.relationshipDao = relationshipDao;
	}

	@Override
	public void run() {
		try {
			cache.sadd(PROCESSED_USERS_KEY, uid);

			String[] friendsIds = relationshipDao.getFriendsIds(uid);
			String[] followersIds = relationshipDao.getFollowersIds(uid);
			save(uid, friendsIds);
			String[] keys = new String[] { WAIT_USERS_KEY, PROCESSED_USERS_KEY, CLOSE_USERS_KEY };
			cache.eval(SADD_IF_NOT_EXIST_Others_script, keys, friendsIds);
			cache.eval(SADD_IF_NOT_EXIST_Others_script, keys, followersIds);
		} catch (Exception e) {
			// 会出现20003错误，表示用户不存在
			logger.error("Error Uid:{}", uid);
		}
	}

	private void save(String uid, String[] friendsIds) {
		StringBuilder value = new StringBuilder();
		for (String friendId : friendsIds) {
			value.append(friendId).append(",");
		}
		if (value.length() > 0) {
			value.deleteCharAt(value.length() - 1);
		}
		writer.write(uid, value.toString());
	}

}