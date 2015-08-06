package zx.soft.weibo.mapred.sina.uids;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.HConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.hbase.api.core.HBaseTable;
import zx.soft.redis.client.cache.Cache;
import zx.soft.weibo.mapred.domain.User;
import zx.soft.weibo.mapred.domain.UsersAndIds;
import zx.soft.weibo.mapred.hdfs.HdfsWriter;
import zx.soft.weibo.mapred.source.SourceId;

public class Spider implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(Spider.class);

	private final SinaRelationshipDao relationshipDao;

	private final HdfsWriter writer;

	private final String uid;

	private final Cache cache;

	private final HConnection conn;

	public static final String CLOSE_USERS_KEY = "sent:sina:closeUsers";

	public static final String PROCESSED_USERS_KEY = "sent:sina:processedUsers";

	public static final String WAIT_USERS_KEY = "sent:sina:waitUsers";

	public static final String SINA_USER_BASEINFO = "sina_user_baseinfo";

	public static final String USER_BASEINFO_COLUMNFAMILIES = "sina_domain";

	public static final String SINA_USER_SCORE = "sina_user_score";

	public static final String USER_SCORE_COLUMNFAMILIES = "score";

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
			HConnection conn) {
		if (StringUtils.isEmpty(uid)) {
			logger.error("Uid is empty!");
			throw new IllegalArgumentException("uid is empty");
		}
		this.uid = uid;
		this.cache = cache;
		this.writer = writer;
		this.relationshipDao = relationshipDao;
		this.conn = conn;
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
		HBaseTable table = null;
		try {
			table = new HBaseTable(conn, Spider.SINA_USER_BASEINFO);
		} catch (IOException e) {
			logger.error("创建HBaseTable实例错误");
			e.printStackTrace();
		}
		if (userAndIds.getIds().size() > 0) {
			List<User> users = userAndIds.getUsers();
			List<String> rowKeys = userAndIds.getIds();
			try {
				table.putObjects(rowKeys, Spider.USER_BASEINFO_COLUMNFAMILIES, users);
			} catch (InstantiationException e) {
				logger.error("InstantiationException 插入失败");
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				logger.error("IllegalAccessException 插入失败");
				e.printStackTrace();
			} catch (IOException e) {
				logger.error("IOException 插入失败");
				e.printStackTrace();
			}

		}
		table.close();
	}

	private void saveUserScore(UsersAndIds userAndIds) throws IOException {
		HBaseTable table = null;
		try {
			table = new HBaseTable(conn, Spider.SINA_USER_SCORE);
		} catch (IOException e) {
			logger.error("创建HBaseTable实例错误");
			e.printStackTrace();
		}
		if (userAndIds.getIds().size() > 0) {
			for (User user : userAndIds.getUsers()) {
				long created_time = user.getCreated_at().getTime();
				int statuses_count = user.getStatuses_count();
				//1243785600为2009年6.1日时间戳,与现在的时间差194955s1779850265
				double score = (double) (statuses_count + 1) / ((created_time - 1243785600) / 86400000);
				//存储<score，id>键值对
				table.put(user.getIdstr(), Spider.USER_SCORE_COLUMNFAMILIES, "active", String.valueOf(score));
				logger.info(user.getIdstr() + ":" + score + " put  success");
			}
		}
		table.close();
	}

}
