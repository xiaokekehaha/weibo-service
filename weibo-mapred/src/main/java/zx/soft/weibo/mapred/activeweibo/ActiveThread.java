package zx.soft.weibo.mapred.activeweibo;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.redis.client.cache.Cache;
import zx.soft.utils.http.ClientDao;
import zx.soft.utils.json.JsonUtils;
import zx.soft.weibo.mapred.domain.Weibo;
import zx.soft.weibo.mapred.utils.Constant;
import zx.soft.weibo.mapred.utils.SinaDomainUtils;
import zx.soft.weibo.sina.api.SinaWeiboAPI;
import zx.soft.weibo.sina.domain.SinaDomain;

public class ActiveThread implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(ActiveThread.class);
	public static final String UPDATED_WEIBO_ACTIVE_USER = "sent:sina:updatedWeiboActiveUser";
	public static final String ACTIVE_USER = "sent:sina:activeUser";
	private String sinceId;
	private ClientDao clientDao;
	private Cache cache;

	public ActiveThread(String sinceId, ClientDao clientDao, Cache cache) {
		this.sinceId = sinceId;
		this.clientDao = clientDao;
		this.cache = cache;
	}

	@Override
	public void run() {
		try {
			List<String> uids = new ArrayList<>();
			for (int i = 0; i < 100; i++) {
				String uid = cache.spop(ActiveThread.ACTIVE_USER);
				if (uid != null) {
					uids.add(uid);
				}
			}
			cache.sadd(ActiveThread.UPDATED_WEIBO_ACTIVE_USER, uids.toArray(new String[0]));
			List<Weibo> weibos = getActiveUserWeibo(uids, sinceId);
			if (weibos.size() > 0) {
				logger.info(clientDao.doPostAndPutKeepAlive(Constant.WEIBO_LASTEST_POST,
						JsonUtils.toJsonWithoutPretty(weibos)));
				logger.info("post to lastest weibo and size=" + weibos.size());
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

	}

	private List<Weibo> getActiveUserWeibo(List<String> ids, String since_id) {
		SinaWeiboAPI api = new SinaWeiboAPI(clientDao);
		List<Weibo> weibos = new ArrayList<>();
		for (String id : ids) {
			SinaDomain sinaDomain = api.statusesUserTimelineByUid(id, since_id, "0", 10, 1, 0, 0, 0);
			List<Weibo> weibo = null;
			if (sinaDomain.containsKey("error_code")
					&& sinaDomain.getFieldValue("error_code").toString().equals("10022")) {
				logger.error("IP requests out of rate limit，return null");
				try {
					logger.info("sleep 1 hours");
					Thread.sleep(3600_000);
					logger.info("sleep over,retry...");
					sinaDomain = api.statusesUserTimelineByUid(id, since_id, "0", 10, 1, 0, 0, 0);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			if (sinaDomain != null && sinaDomain.containsKey("statuses")) {
				weibo = SinaDomainUtils.getUserWeibos(sinaDomain);
				weibos.addAll(weibo);
			}
		}
		return weibos;
	}

}
