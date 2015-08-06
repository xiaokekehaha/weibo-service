package zx.soft.weibo.mapred.activeweibo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.utils.http.ClientDao;
import zx.soft.utils.http.HttpClientDaoImpl;
import zx.soft.weibo.mapred.domain.Weibo;
import zx.soft.weibo.mapred.utils.SinaDomainUtils;
import zx.soft.weibo.sina.api.SinaWeiboAPI;
import zx.soft.weibo.sina.domain.SinaDomain;

public class ActiveUser {

	private static Logger logger = LoggerFactory.getLogger(ActiveUser.class);
	private static final String BASE_URL = "http://192.168.6.126:8888/users/active/";

	private ClientDao clientDao;
	private SinaWeiboAPI api;

	public ActiveUser(ClientDao clientDao) {
		this.clientDao = clientDao;
		this.api = new SinaWeiboAPI(clientDao);
	}

	/*
	 * 获取某个用户最新发表的微博列表
	 * uid:需要查询的用户ID。
	 * screen_name:需要查询的用户昵称。
	 * since_id:若指定此参数，则返回ID比since_id大的微博（即比since_id时间晚的微博），默认为0。
	 * max_id:若指定此参数，则返回ID小于或等于max_id的微博，默认为0。
	 * count:单页返回的记录条数，最大不超过100，超过100以100处理，默认为20。
	 * page:返回结果的页码，默认为1。
	 * base_app:是否只获取当前应用的数据。0为否（所有数据），1为是（仅当前应用），默认为0。
	 * feature	:过滤类型ID，0：全部、1：原创、2：图片、3：视频、4：音乐，默认为0。
	 * trim_user:返回值中user字段开关，0：返回完整user字段、1：user字段仅返回user_id，默认为0。
	 */
	public List<Weibo> getActiveUserWeibo(List<String> ids) {
		List<Weibo> weibos = new ArrayList<>();
		for (String id : ids) {
			SinaDomain sinaDomain = api.statusesUserTimelineByUid(id, "0", "0", 10, 1, 0, 0, 0);
			List<Weibo> weibo = null;
			if (sinaDomain.containsKey("statuses")) {
				weibo = SinaDomainUtils.getUserWeibos(sinaDomain);
				logger.info(id + ":" + weibo.toString());
				weibos.addAll(weibo);
			}
		}
		return weibos;
	}

	public List<String> getActiveUserId(int from) {
		List<String> topN = null;
		String users = clientDao.doGet(BASE_URL + from);
		String[] ids = users.replace("\"", "").replace("[", "").replace("]", "").split(",");
		topN = Arrays.asList(ids);
		return topN;
	}

	public static void main(String[] args) {
		ActiveUser users = new ActiveUser(new HttpClientDaoImpl());
		List<String> ids = users.getActiveUserId(0);
		List<Weibo> weibos = users.getActiveUserWeibo(ids);
		System.out.println(weibos.size());
	}
}
