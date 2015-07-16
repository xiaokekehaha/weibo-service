package zx.soft.weibo.mapred.sina.uids;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.hbase.api.core.HBaseTable;
import zx.soft.redis.client.cache.Cache;
import zx.soft.weibo.mapred.domain.User;
import zx.soft.weibo.mapred.domain.UsersAndIds;
import zx.soft.weibo.mapred.hdfs.HdfsWriter;
import zx.soft.weibo.mapred.sina.FriendshipsDetail;
import zx.soft.weibo.sina.api.SinaWeiboAPI;

public class Spider implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(Spider.class);

	private final SinaRelationshipDao relationshipDao;

	private final HdfsWriter writer;

	private final String uid;

	private final Cache cache;

	private final HBaseTable table;

	public static final String CLOSE_USERS_KEY = "sent:sina:closeUsers";

	public static final String PROCESSED_USERS_KEY = "sent:sina:processedUsers";

	public static final String WAIT_USERS_KEY = "sent:sina:waitUsers";

	public static final String SINA_USER_BASEINFO = "sina_user_baseinfo";

	public static final String USER_BASEINFO_COLUMNFAMILIES = "sina_domain";

	private static final AtomicInteger COUNT = new AtomicInteger(0);

	private static final String COOKIE = "SINAGLOBAL=1967857952695.3398.1430183667535; _s_tentry=login.sina.com.cn; Apache=6243587974458.933.1433726825631; ULV=1433726825660:3:1:1:6243587974458.933.1433726825631:1430816662844; login_sid_t=001965a1c28c55a224e738dda5783e23; un=wgxzy_1015@163.com; UOR=,,ent.ifeng.com; SUS=SID-1862087393-1436238674-GZ-0whpq-03ce46c5672abef5da25811248d5e4bd; SUE=es%3D671697a1ce8c8ad7ef60d07055dc7616%26ev%3Dv1%26es2%3D331cd66d1fcc0355bf19c60d6b91e47d%26rs0%3DaghTVO4Ej445DRI5nES%252BxWyKQHtytQDMXMkCJUL4HssQE3XNhws01QHrJmgZAm5UECfPOyzTrfECWCK5Et5lFJ7zNFXP7mIH3ir6AtNo2OMCzRxokAJ82gKlBlq%252BUDyScFj1%252BjsUcBK1AqVLe1TXFREOQ3zLUenrnt5gH9yG1gM%253D%26rv%3D0; SUP=cv%3D1%26bt%3D1436238674%26et%3D1436325074%26d%3Dc909%26i%3De4bd%26us%3D1%26vf%3D0%26vt%3D0%26ac%3D2%26st%3D0%26uid%3D1862087393%26name%3Dwgxzy_1015%2540sina.com%26nick%3D%25E6%25B0%25B8%25E4%25B8%258D%25E6%25AD%25A2%25E6%25AD%25A5%26fmp%3D%26lcp%3D2012-03-12%252000%253A03%253A03; SUB=_2A254nzMCDeTxGedG7VAR-CnPwj-IHXVb7SPKrDV8PUNbvtBeLXL9kW8-SPdIX1GZubGQKtb1SVsk0ByRNA..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WhOMc47RCc5Rfeu-vvUZEJ45JpX5KMt; SUHB=05iLVP-ro1wVou; ALF=1467774670; SSOLoginState=1436238674; JSESSIONID=35410988D39244A4CCE2D7ACB7BCE352";

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
			HBaseTable table) {
		if (StringUtils.isEmpty(uid)) {
			logger.error("Uid is empty!");
			throw new IllegalArgumentException("uid is empty");
		}
		this.uid = uid;
		this.cache = cache;
		this.writer = writer;
		this.relationshipDao = relationshipDao;
		this.table = table;
	}

	@Override
	public void run() {
		try {
			// 记录请求Uid次数
			logger.info("Retriving uids' count:{}", COUNT.addAndGet(1));
			cache.sadd(PROCESSED_USERS_KEY, uid);
			System.out.println("当前uid" + uid);
			// 分别获取用户关注和粉丝详细数据
			UsersAndIds friends = relationshipDao.getFriends(uid);
			UsersAndIds followers = relationshipDao.getFollowers(uid);
			// 判断是否为空
			if (friends != null && followers != null) {
				/******************用户详细信息列表存入HBase*****************/
				save2HBase(friends);
				/******************用户ID信息列表存入Redis集群*****************/
				save(uid, friends.getIds().toArray(new String[0]));
				String[] keys = new String[] { WAIT_USERS_KEY, PROCESSED_USERS_KEY, CLOSE_USERS_KEY };
				cache.eval(SADD_IF_NOT_EXIST_Others_script, keys, friends.getIds().toArray(new String[0]));
				cache.eval(SADD_IF_NOT_EXIST_Others_script, keys, followers.getIds().toArray(new String[0]));
			}
		} catch (Exception e) {
			if (e.getMessage().contains(FriendshipsDetail.IP_REQUEST_LIMIT)) {
				SinaWeiboAPI.superid = nextSuperId();
				try {
					UsersAndIds friends = relationshipDao.getFriends(uid);
					UsersAndIds followers = relationshipDao.getFollowers(uid);
					// 判断是否为空
					if (friends != null && followers != null) {
						/******************用户详细信息列表存入HBase*****************/
						save2HBase(friends);
						/******************用户ID信息列表存入Redis集群*****************/
						save(uid, friends.getIds().toArray(new String[0]));
						String[] keys = new String[] { WAIT_USERS_KEY, PROCESSED_USERS_KEY, CLOSE_USERS_KEY };
						cache.eval(SADD_IF_NOT_EXIST_Others_script, keys, friends.getIds().toArray(new String[0]));
						cache.eval(SADD_IF_NOT_EXIST_Others_script, keys, followers.getIds().toArray(new String[0]));
					}
				} catch (Exception e1) {
					if (e.getMessage().contains(FriendshipsDetail.IP_REQUEST_LIMIT)) {
						logger.info("use up superid,now sleeping");
						try {
							Thread.sleep(3600_1000);
						} catch (InterruptedException e2) {
							e2.printStackTrace();
						}
					}
				}
			}
			// 多种异常，需要通过错误码识别处理
			// TODO
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

	private void save2HBase(UsersAndIds userAndIds) throws InstantiationException, IllegalAccessException, IOException {
		List<User> users = userAndIds.getUsers();
		if (users.size() > 0) {
			for (User user : users) {
				table.putObject(user.getIdstr(), Spider.USER_BASEINFO_COLUMNFAMILIES, user);
			}
		}

	}

}
