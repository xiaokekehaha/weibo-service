package zx.soft.weibo.sina.example;

import zx.soft.utils.http.HttpClientDaoImpl;
import zx.soft.utils.json.JsonUtils;
import zx.soft.weibo.sina.api.SinaWeiboAPI;
import zx.soft.weibo.sina.domain.SinaDomain;

/**
 * 获取某个用户最新发表的微博列表
 * 注意：需要使用特定的superid
 *
 * @author wanggang
 *
 */
public class StatusesUserTimeline {

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
	public static void main(String[] args) {

		SinaWeiboAPI api = new SinaWeiboAPI(new HttpClientDaoImpl());
		SinaDomain sinaDomain = api.statusesUserTimelineByUid("1732243641", "0", "0", 5, 1, 0, 0, 0);
		//SinaDomain sinaDomain = api.statusesUserTimelineByScreenName("付强bber", "0", "0", 20, 1, 0, 0, 0);
		System.out.println(JsonUtils.toJson(sinaDomain));
		//3872946600794464
	}
}
