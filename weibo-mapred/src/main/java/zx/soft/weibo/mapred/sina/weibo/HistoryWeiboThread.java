package zx.soft.weibo.mapred.sina.weibo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.hbase.api.core.HBaseTable;
import zx.soft.weibo.mapred.domain.Weibo;
import zx.soft.weibo.mapred.source.SourceId;

public class HistoryWeiboThread implements Runnable {
	public static Logger logger = LoggerFactory.getLogger(HistoryWeiboThread.class);
	private List<String> uids;
	private HConnection conn;
	public static final String History_Weibo = "history_weibo";
	public static final String History_Weibo_ColumnFamily = "sina_weibo";

	public HistoryWeiboThread(List<String> uids, HConnection conn) {
		this.uids = uids;
		this.conn = conn;
	}

	@Override
	public void run() {
		String source = SourceId.getFirstUseful();
		for (String uid : uids) {
			List<Weibo> weibos = new ArrayList<>();
			while ((weibos = HistoryWeibo.getHistoryWeibos(uid, source)) == null) {
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
				save(weibos);
			}
		}
	}

	private void save(List<Weibo> weibos) {
		HBaseTable table = null;
		try {
			table = new HBaseTable(conn, HistoryWeiboThread.History_Weibo);
		} catch (IOException e) {
			logger.error("创建HBaseTable实例错误");
			e.printStackTrace();
		}
		for (Weibo weibo : weibos) {
			try {
				table.putObject(weibo.getIdstr(), HistoryWeiboThread.History_Weibo_ColumnFamily, weibo);
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
