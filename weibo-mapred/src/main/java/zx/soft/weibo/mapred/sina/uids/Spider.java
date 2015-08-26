package zx.soft.weibo.mapred.sina.uids;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.redis.client.cache.Cache;
import zx.soft.utils.http.ClientDao;
import zx.soft.utils.json.JsonUtils;
import zx.soft.weibo.mapred.domain.User;
import zx.soft.weibo.mapred.domain.UsersAndIds;
import zx.soft.weibo.mapred.hdfs.HdfsWriter;
import zx.soft.weibo.mapred.source.SourceId;
import zx.soft.weibo.mapred.utils.Constant;

public class Spider implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(Spider.class);

	private final SinaRelationshipDao relationshipDao;

	private final HdfsWriter writer;

	private final String uid;

	private final Cache cache;

	private final ClientDao client;

	public static final String CLOSE_USERS_KEY = "sent:sina:closeUsers";

	public static final String PROCESSED_USERS_KEY = "sent:sina:processedUsers";

	public static final String WAIT_USERS_KEY = "sent:sina:waitUsers";

	public static int key = 0;

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

	public Spider(String uid, final Cache cache, HdfsWriter writer, SinaRelationshipDao relationshipDao,
			ClientDao client) {
		if (StringUtils.isEmpty(uid)) {
			logger.error("Uid is empty!");
			throw new IllegalArgumentException("uid is empty");
		}
		this.uid = uid;
		this.cache = cache;
		this.writer = writer;
		this.relationshipDao = relationshipDao;
		this.client = client;
	}

	@Override
	public void run() {

		String source = SourceId.getFirstUseful();
		try {
			cache.sadd(PROCESSED_USERS_KEY, uid);
			// 分别获取用户关注和粉丝详细数据
			UsersAndIds friends = relationshipDao.getFriends(uid, source);
			UsersAndIds followers = relationshipDao.getFollowers(uid, source);
			// 判断是否为空
			if (friends != null && followers != null) {
				/******************用户详细信息列表存入HBase*****************/
				save2HBase(friends);
				/***************将用户活跃度得分存入hbase表*****************/
				saveUserScore(friends);
				/******************用户ID信息列表存入Redis集群*****************/
				save(uid, friends.getIds().toArray(new String[0]));
				String[] keys = new String[] { WAIT_USERS_KEY, PROCESSED_USERS_KEY, CLOSE_USERS_KEY };
				cache.eval(SADD_IF_NOT_EXIST_Others_script, keys, friends.getIds().toArray(new String[0]));
				cache.eval(SADD_IF_NOT_EXIST_Others_script, keys, followers.getIds().toArray(new String[0]));
			}

		} catch (Exception e) {
			// 多种异常，需要通过错误码识别处理
			e.printStackTrace();
			logger.error("Current Error Uid:{}", uid + ";error message:" + e.getMessage());
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

	private void save2HBase(UsersAndIds userAndIds) {
		if (userAndIds.getIds().size() > 0) {
			client.doPost(Constant.USER_INFO_POST, JsonUtils.toJsonWithoutPretty(userAndIds.getUsers()));
		}
	}

	private void saveUserScore(UsersAndIds userAndIds) throws IOException {
		if (userAndIds.getIds().size() > 0) {
			Map<String, String> ids_scores = new HashMap<>();
			for (User user : userAndIds.getUsers()) {
				long created_time = user.getCreated_at().getTime();
				int statuses_count = user.getStatuses_count();
				//1243785600为2009年6.1日时间戳,与现在的时间差194955s1779850265
				double score = (double) (statuses_count + 1) / ((created_time - 1243785600) / 86400000);
				ids_scores.put(user.getIdstr(), String.valueOf(score));
				client.doPost(Constant.USER_SCORE_POST, JsonUtils.toJsonWithoutPretty(ids_scores));
			}
		}
	}

}
