package zx.soft.weibo.mapred.sina.weibo;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.redis.client.cache.Cache;
import zx.soft.utils.http.ClientDao;
import zx.soft.utils.json.JsonUtils;
import zx.soft.weibo.mapred.domain.Weibo;
import zx.soft.weibo.mapred.source.SourceId;
import zx.soft.weibo.mapred.utils.Constant;
import zx.soft.weibo.sina.api.SinaWeiboAPI;

public class HistoryWeiboThread implements Runnable {
	public static Logger logger = LoggerFactory.getLogger(HistoryWeiboThread.class);
	private ClientDao client;
	private String uid;
	private Cache cache;
	public static final String key = "sent:sina:historyUids";

	public HistoryWeiboThread(String uid, ClientDao client, Cache cache) {
		this.client = client;
		this.uid = uid;
		this.cache = cache;
	}

	@Override
	public void run() {

		try {
			cache.sadd(key, uid);
			String source = SourceId.getFirstUseful();
			List<Weibo> weibos = new ArrayList<>();
			SinaWeiboAPI api = new SinaWeiboAPI(client);
			weibos = HistoryWeibo.getHistoryWeibos(uid, source, api);
			//logger.info(uid + ":" + weibos.size());

			if (weibos.size() > 0) {
				List<Weibo> wei = new ArrayList<>();
				for (int i = 0; i < weibos.size(); i++) {
					if (((i + 1) % 50) == 0) {
						client.doPostAndPutKeepAlive(Constant.WEIBO_HISTORY_POST, JsonUtils.toJsonWithoutPretty(wei));
						//logger.info( + ";size=" + wei.size()));
						wei.clear();
					}
					wei.add(weibos.get(i));
				}
				client.doPostAndPutKeepAlive(Constant.WEIBO_HISTORY_POST, JsonUtils.toJsonWithoutPretty(wei));
				//logger.info(	+ ";size=" + wei.size()));
				wei.clear();
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}

	}
}
