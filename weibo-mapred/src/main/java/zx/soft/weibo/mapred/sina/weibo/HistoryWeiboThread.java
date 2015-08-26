package zx.soft.weibo.mapred.sina.weibo;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.utils.http.ClientDao;
import zx.soft.utils.json.JsonUtils;
import zx.soft.weibo.mapred.domain.Weibo;
import zx.soft.weibo.mapred.source.SourceId;
import zx.soft.weibo.mapred.utils.Constant;
import zx.soft.weibo.sina.api.SinaWeiboAPI;

public class HistoryWeiboThread implements Runnable {
	public static Logger logger = LoggerFactory.getLogger(HistoryWeiboThread.class);
	private List<String> uids;
	private ClientDao client;

	public HistoryWeiboThread(List<String> uids, ClientDao client) {
		this.uids = uids;
		this.client = client;
	}

	@Override
	public void run() {
		String source = SourceId.getFirstUseful();
		for (String uid : uids) {
			List<Weibo> weibos = new ArrayList<>();
			SinaWeiboAPI api = new SinaWeiboAPI(client);
			while ((weibos = HistoryWeibo.getHistoryWeibos(uid, source, api)) == null) {
				logger.info("change source=" + source);
				logger.info("达到次数限制，睡眠１小时");
				try {
					Thread.sleep(3600_1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				source = SourceId.getFirstUseful();
			}
			logger.info(uid + ":" + weibos.size());
			if (weibos.size() > 0) {
				client.doPost(Constant.WEIBO_HISTORY_POST, JsonUtils.toJsonWithoutPretty(weibos));
			}
		}
	}

}
