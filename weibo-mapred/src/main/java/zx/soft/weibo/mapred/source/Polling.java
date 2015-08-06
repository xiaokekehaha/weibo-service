package zx.soft.weibo.mapred.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.utils.http.HttpClientDaoImpl;
import zx.soft.weibo.mapred.sina.FriendshipsDetail;
import zx.soft.weibo.sina.api.SinaWeiboAPI;
import zx.soft.weibo.sina.domain.SinaDomain;

public class Polling implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(Polling.class);
	SinaWeiboAPI api = new SinaWeiboAPI(new HttpClientDaoImpl());

	@Override
	public void run() {

		while (true) {

			if (SourceId.getUselesses() != null && SourceId.getLenUseless() > 0) {
				for (String source : SourceId.getUselesses()) {
					SinaDomain sinaDomian = api.friendshipsFollowers("1642591402", 10, 0, 1, source);
					if (!sinaDomian.getFieldValue("error_code").equals(FriendshipsDetail.IP_REQUEST_LIMIT)) {
						logger.info("add source" + source + " to useful list");
						SourceId.addIdUseful(source);
					}
				}

			}

			try {
				logger.info("Polling sleep 5 mins");
				Thread.sleep(5 * 60 * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
	}
}
