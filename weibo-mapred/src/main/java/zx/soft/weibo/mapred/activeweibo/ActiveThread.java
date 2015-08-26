package zx.soft.weibo.mapred.activeweibo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.utils.http.ClientDao;
import zx.soft.utils.json.JsonUtils;
import zx.soft.weibo.mapred.domain.Weibo;
import zx.soft.weibo.mapred.utils.Constant;
import zx.soft.weibo.mapred.utils.SinaDomainUtils;
import zx.soft.weibo.sina.api.SinaWeiboAPI;
import zx.soft.weibo.sina.domain.SinaDomain;

public class ActiveThread implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(ActiveThread.class);

	private int from;
	private String sinceId;
	private ClientDao clientDao;

	public ActiveThread(int from, String sinceId, ClientDao clientDao) {
		this.from = from;
		this.sinceId = sinceId;
		this.clientDao = clientDao;
	}

	@Override
	public void run() {
		List<String> uids = getActiveUserId(from);
		List<Weibo> weibos = getActiveUserWeibo(uids, sinceId);
		if (weibos.size() > 0) {
			clientDao.doPost(Constant.WEIBO_LASTEST_POST, JsonUtils.toJsonWithoutPretty(weibos));
			logger.info("post to lastest weibo and size=" + weibos.size());
		}
	}

	private List<Weibo> getActiveUserWeibo(List<String> ids, String since_id) {
		SinaWeiboAPI api = new SinaWeiboAPI(clientDao);
		List<Weibo> weibos = new ArrayList<>();
		for (String id : ids) {
			SinaDomain sinaDomain = api.statusesUserTimelineByUid(id, since_id, "0", 10, 1, 0, 0, 0);
			List<Weibo> weibo = null;
			if (sinaDomain.containsKey("statuses")) {
				weibo = SinaDomainUtils.getUserWeibos(sinaDomain);
				weibos.addAll(weibo);
			}
		}
		return weibos;
	}

	private List<String> getActiveUserId(int from) {
		List<String> topN = null;
		String users = clientDao.doGet(Constant.USER_SCORE_GET + from);
		String[] ids = users.replace("\"", "").replace("[", "").replace("]", "").split(",");
		topN = Arrays.asList(ids);
		return topN;
	}

}
