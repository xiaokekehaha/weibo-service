package zx.soft.weibo.mapred.activeweibo;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.HConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.hbase.api.core.HBaseTable;
import zx.soft.utils.http.HttpClientDaoImpl;
import zx.soft.weibo.mapred.domain.Weibo;

public class ActiveThread implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(ActiveThread.class);
	public static final String ACTIVE_USERS_LASTEST_WEIBOS = "active_user_lastest_weibos";
	public static final String ACTIVE_USERS_LASTEST_WEIBOS_COLUMNFAMILY = "lastest_weibos";
	private ActiveUser activeUser;
	private int from;
	private HConnection conn;
	private String sinceId;

	public ActiveThread(int from, HConnection conn, String sinceId) {
		this.activeUser = new ActiveUser(new HttpClientDaoImpl());
		this.from = from;
		this.conn = conn;
		this.sinceId = sinceId;
	}

	@Override
	public void run() {
		List<String> uids = activeUser.getActiveUserId(from);
		List<Weibo> weibos = activeUser.getActiveUserWeibo(uids, sinceId);
		//存储获得的微博信息
		save(weibos);
	}

	private void save(List<Weibo> weibos) {
		HBaseTable table = null;
		try {
			table = new HBaseTable(conn, ActiveThread.ACTIVE_USERS_LASTEST_WEIBOS);
		} catch (IOException e) {
			logger.error("创建HBaseTable实例错误");
			e.printStackTrace();
		}
		for (Weibo weibo : weibos) {
			try {
				table.putObject(weibo.getIdstr(), ActiveThread.ACTIVE_USERS_LASTEST_WEIBOS_COLUMNFAMILY, weibo);
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		table.close();
	}
}
