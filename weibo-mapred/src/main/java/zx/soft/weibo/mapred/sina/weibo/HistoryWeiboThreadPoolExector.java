package zx.soft.weibo.mapred.sina.weibo;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;

import zx.soft.utils.config.ConfigUtil;
import zx.soft.utils.http.ClientDao;
import zx.soft.utils.http.HttpClientDaoImpl;
import zx.soft.weibo.mapred.source.SourceId;
import zx.soft.weibo.mapred.utils.ThreadPoolExecutorUtils;

import com.google.protobuf.ServiceException;

public class HistoryWeiboThreadPoolExector {

	static {
		Properties props = ConfigUtil.getProps("super.properties");
		for (String id : props.getProperty("super_user_timeline").split(",")) {
			SourceId.addIdUseful(id);
		}
	}

	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException,
			ServiceException, InterruptedException {
		int cpuNums = 64;
		ThreadPoolExecutor pool = ThreadPoolExecutorUtils.createExecutor(cpuNums);
		List<List<String>> lists = UidsList.getSubList();
		ClientDao client = new HttpClientDaoImpl();
		for (int i = 0; i < lists.size(); i++) {
			pool.execute(new HistoryWeiboThread(lists.get(i), client));
		}
		pool.shutdown();
	}
}
