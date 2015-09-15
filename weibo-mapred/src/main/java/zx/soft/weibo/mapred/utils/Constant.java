package zx.soft.weibo.mapred.utils;

import java.util.Properties;

import zx.soft.utils.config.ConfigUtil;

public class Constant {

	private static String target;

	static {
		Properties props = ConfigUtil.getProps("server.properties");
		target = "http://" + props.getProperty("server.ip") + ":" + props.getProperty("server.port");
	}
	//活跃用户最新微博信息表
	public static final String LASTEST_WEIBO_TABLE = "lastest_weibo";
	//历史微博表
	public static final String HISTORY_WEIBO_TABLE = "history_weibo";
	//用户信息表
	public static final String USER_TABLE = "user";
	//用户活跃度得分表
	public static final String USER_SCORE_TABLE = "user_score";

	public static final String HISTORY_WEIBO_CF = "history";
	public static final String LASTEST_WEIBO_CF = "lastest";
	public static final String USER_CF = "user";
	public static final String USER_SCORE_CF = "user_score";

	public static final String USER_SCORE_Q = "score";

	public static final String USER_INFO_POST = target + "/hbase/users/info";
	public static final String WEIBO_HISTORY_POST = target + "/hbase/weibos/history";
	public static final String WEIBO_LASTEST_POST = target + "/hbase/weibos/lastest";
	public static final String USER_SCORE_POST = target + "/hbase/users/score";
	public static final String USER_SCORE_GET = target + "/users/active/";
	public static final String MAX_WEIBO_ID_GET = target + "/weibos/active/maxid";
}
