package zx.soft.weibo.mapred.sina.weibo;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.utils.http.HttpClientDaoImpl;
import zx.soft.weibo.mapred.domain.Weibo;
import zx.soft.weibo.mapred.utils.SinaDomainUtils;
import zx.soft.weibo.sina.api.SinaWeiboAPI;
import zx.soft.weibo.sina.domain.SinaDomain;

public class HistoryWeibo {

	private static Logger logger = LoggerFactory.getLogger(HistoryWeibo.class);
	private static final int PAGE_COUNT = 10;

	public static List<Weibo> getHistoryWeibos(String uid, String source, SinaWeiboAPI api) {
		int page = 1;
		List<Weibo> weibos = new ArrayList<>();
		boolean flag = true;
		while (flag) {
			List<Weibo> weibo = null;
			SinaDomain sinaDomain = api.statusesUserTimelineByUid(uid, "0", "0", PAGE_COUNT, page++, 0, 0, 0, source);
			if (sinaDomain.containsKey("error_code")
					&& sinaDomain.getFieldValue("error_code").toString().equals("10022")) {
				logger.error("IP requests out of rate limit，return null");
				try {
					logger.info("sleep 1 hours");
					Thread.sleep(3600_000);
					logger.info("sleep over,retry...");
					sinaDomain = api.statusesUserTimelineByUid(uid, "0", "0", PAGE_COUNT, page, 0, 0, 0, source);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			if (sinaDomain != null) {
				weibo = SinaDomainUtils.getUserWeibos(sinaDomain);
				if (weibo.size() > 0) {
					weibos.addAll(weibo);
				} else {
					flag = false;
				}
			}
		}
		return weibos;
	}

	public static void main(String[] args) {
		SinaWeiboAPI api = new SinaWeiboAPI(new HttpClientDaoImpl());
		List<Weibo> weibos = HistoryWeibo.getHistoryWeibos("5110432155", "3442868347", api);
		System.out.println(weibos.size());
	}
}
