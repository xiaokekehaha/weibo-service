package zx.soft.weibo.mapred.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.weibo.mapred.domain.User;
import zx.soft.weibo.mapred.domain.UsersAndIds;
import zx.soft.weibo.mapred.domain.Visible;
import zx.soft.weibo.mapred.domain.Weibo;
import zx.soft.weibo.sina.domain.SinaDomain;

/**
 * 新浪对象转换工具
 *
 * @author wanggang
 *
 */
public class SinaDomainUtils {

	private static Logger logger = LoggerFactory.getLogger(SinaDomainUtils.class);

	public static List<Weibo> getUserWeibos(SinaDomain sinaDomain) {
		List<Weibo> weibos = new ArrayList<>();
		SinaDomain tmp = null;
		if (sinaDomain.containsKey("statuses") && sinaDomain.getFieldValues("statuses").size() > 0
				&& (!sinaDomain.isEmpty())) {
			for (Object weibo : sinaDomain.getFieldValues("statuses")) {
				try {
					tmp = (SinaDomain) weibo;
					weibos.add(new Weibo.Builder(getString(tmp, "id"), getString(tmp, "mid"), getString(tmp, "idstr"),
							dateChange(getString(tmp, "created_at"))).setText(getString(tmp, "text"))
							.setSource_allowclick(getInt(tmp, "source_allowclick"))
							.setSource_type(getInt(tmp, "source_type")).setSource(getString(tmp, "source"))
							.setFavorited(getBoolean(tmp, "favorited")).setTruncated(getBoolean(tmp, "truncated"))
							.setIn_reply_to_status_id(getString(tmp, "in_reply_to_status_id"))
							.setIn_reply_to_user_id(getString(tmp, "in_reply_to_user_id"))
							.setIn_reply_to_screen_name(getString(tmp, "in_reply_to_screen_name"))
							.setPic_urls(getList(tmp, "pic_urls")).setGeo(getString(tmp, "geo"))
							.setUser(getUser(tmp, "user")).setReposts_count(getInt(tmp, "reposts_count"))
							.setComments_count(getInt(tmp, "comments_count"))
							.setAttitudes_count(getInt(tmp, "attitudes_count")).setMlevel(getInt(tmp, "mlevel"))
							.setVisible(getVisible(tmp, "visible")).setDarwin_tags(getList(tmp, "darwin_tags"))
							.setOwid(getLong(tmp, "owid")).setOusername(getLong(tmp, "ousername")).build());
				} catch (ClassCastException e) {
					if (weibo.toString().equals("[]")) {
						logger.info("已爬取所有微博,后续请求返回为" + weibo.toString());
					}
					return weibos;
				}

			}
		}
		return weibos;
	}

	public static UsersAndIds getUsersAndIds(SinaDomain sinaDomain) {
		List<User> users = new ArrayList<>();
		List<String> ids = new ArrayList<>();
		SinaDomain tmp = null;
		if (sinaDomain.containsKey("users") && sinaDomain.getFieldValues("users").size() > 0) {
			for (Object friend : sinaDomain.getFieldValues("users")) {
				try {
					tmp = (SinaDomain) friend;
					ids.add(getString(tmp, "idstr"));
					users.add(new User.Builder(getLong(tmp, "id"), getString(tmp, "idstr"), getString(tmp,
							"screen_name"), getString(tmp, "name"), dateChange(getString(tmp, "created_at")))
							.setUclass(getInt(tmp, "class")).setProvince(getInt(tmp, "province"))
							.setCity(getInt(tmp, "city")).setLocation(getString(tmp, "location"))
							.setDescription(getString(tmp, "description")).setUrl(getString(tmp, "url"))
							.setProfile_image_url(getString(tmp, "profile_image_url"))
							.setProfile_url(getString(tmp, "profile_url")).setDomain(getString(tmp, "domain"))
							.setWeihao(getString(tmp, "weihao")).setGender(getString(tmp, "gender"))
							.setFollowers_count(getInt(tmp, "followers_count"))
							.setFriends_count(getInt(tmp, "friends_count"))
							.setPagefriends_count(getInt(tmp, "pagefriends_count"))
							.setStatuses_count(getInt(tmp, "statuses_count"))
							.setFavourites_count(getInt(tmp, "favourites_count"))
							.setFollowing(getBoolean(tmp, "following"))
							.setAllow_all_act_msg(getBoolean(tmp, "allow_all_act_msg"))
							.setGeo_enabled(getBoolean(tmp, "geo_enabled")).setVerified(getBoolean(tmp, "verified"))
							.setVerified_type(Integer.parseInt(tmp.getFieldValue("verified_type").toString()))
							.setRemark(getString(tmp, "remark")).setPtype(getInt(tmp, "ptype"))
							.setAllow_all_comment(getBoolean(tmp, "allow_all_comment"))
							.setAvatar_large(getString(tmp, "avatar_large")).setAvatar_hd(getString(tmp, "avatar_hd"))
							.setVerified_reason(getString(tmp, "verified_reason"))
							.setVerified_trade(getInt(tmp, "verified_trade"))
							.setVerified_reason_url(getString(tmp, "verified_reason_url"))
							.setVerified_source(getString(tmp, "verified_source"))
							.setVerified_source_url(getString(tmp, "verified_source_url"))
							.setVerified_state(getInt(tmp, "verified_state"))
							.setVerified_level(getInt(tmp, "verified_level"))
							.setVerified_reason_modified(getString(tmp, "verified_reason_modified"))
							.setVerified_contact_name(getString(tmp, "verified_contact_name"))
							.setVerified_contact_email(getString(tmp, "verified_contact_email"))
							.setVerified_contact_mobile(getString(tmp, "verified_contact_mobile"))
							.setFollow_me(getBoolean(tmp, "follow_me")).setOnline_status(getInt(tmp, "online_status"))
							.setBi_followers_count(getInt(tmp, "bi_followers_count")).setLang(getString(tmp, "lang"))
							.setStar(getInt(tmp, "star")).setMbtype(getInt(tmp, "mbtype"))
							.setMbrank(getInt(tmp, "mbrank")).setBlock_word(getInt(tmp, "block_word"))
							.setBlock_app(getInt(tmp, "block_app")).setCredit_score(getInt(tmp, "credit_score"))
							.setUser_ability(getInt(tmp, "user_ability")).setUrank(getInt(tmp, "urank")).build());
				} catch (ClassCastException e) {
					logger.error(friend.toString());
					return new UsersAndIds(users, ids);
				}

			}
		}
		return new UsersAndIds(users, ids);
	}

	private static String getString(SinaDomain sinaDomain, String key) {
		try {
			return sinaDomain.getFieldValue(key).toString();
		} catch (Exception e) {
			return "";
		}
	}

	private static int getInt(SinaDomain sinaDomain, String key) {
		try {
			return Integer.parseInt(sinaDomain.getFieldValue(key).toString());
		} catch (Exception e) {
			return 0;
		}
	}

	private static long getLong(SinaDomain sinaDomain, String key) {
		try {
			return Long.parseLong(sinaDomain.getFieldValue(key).toString());
		} catch (Exception e) {
			return 0L;
		}
	}

	private static boolean getBoolean(SinaDomain sinaDomain, String key) {
		try {
			return sinaDomain.getFieldValue(key).toString().equalsIgnoreCase("true") ? Boolean.TRUE : Boolean.FALSE;
		} catch (Exception e) {
			return Boolean.FALSE;
		}
	}

	private static List<String> getList(SinaDomain sinaDomain, String key) {
		List<String> pic_urls = new ArrayList<>();
		Collection<Object> objects = sinaDomain.getFieldValues(key);
		for (Object object : objects) {
			pic_urls.add(object.toString());
		}
		return pic_urls;
	}

	private static User getUser(SinaDomain sinaDomain, String key) {
		SinaDomain tmp = (SinaDomain) sinaDomain.get(key);
		User user = new User.Builder(getLong(tmp, "id"), getString(tmp, "idstr"), getString(tmp, "screen_name"),
				getString(tmp, "name"), dateChange(getString(tmp, "created_at"))).setUclass(getInt(tmp, "class"))
				.setProvince(getInt(tmp, "province")).setCity(getInt(tmp, "city"))
				.setLocation(getString(tmp, "location")).setDescription(getString(tmp, "description"))
				.setUrl(getString(tmp, "url")).setProfile_image_url(getString(tmp, "profile_image_url"))
				.setProfile_url(getString(tmp, "profile_url")).setDomain(getString(tmp, "domain"))
				.setWeihao(getString(tmp, "weihao")).setGender(getString(tmp, "gender"))
				.setFollowers_count(getInt(tmp, "followers_count")).setFriends_count(getInt(tmp, "friends_count"))
				.setPagefriends_count(getInt(tmp, "pagefriends_count"))
				.setStatuses_count(getInt(tmp, "statuses_count")).setFavourites_count(getInt(tmp, "favourites_count"))
				.setFollowing(getBoolean(tmp, "following")).setAllow_all_act_msg(getBoolean(tmp, "allow_all_act_msg"))
				.setGeo_enabled(getBoolean(tmp, "geo_enabled")).setVerified(getBoolean(tmp, "verified"))
				.setVerified_type(Integer.parseInt(tmp.getFieldValue("verified_type").toString()))
				.setRemark(getString(tmp, "remark")).setPtype(getInt(tmp, "ptype"))
				.setAllow_all_comment(getBoolean(tmp, "allow_all_comment"))
				.setAvatar_large(getString(tmp, "avatar_large")).setAvatar_hd(getString(tmp, "avatar_hd"))
				.setVerified_reason(getString(tmp, "verified_reason")).setVerified_trade(getInt(tmp, "verified_trade"))
				.setVerified_reason_url(getString(tmp, "verified_reason_url"))
				.setVerified_source(getString(tmp, "verified_source"))
				.setVerified_source_url(getString(tmp, "verified_source_url"))
				.setVerified_state(getInt(tmp, "verified_state")).setVerified_level(getInt(tmp, "verified_level"))
				.setVerified_reason_modified(getString(tmp, "verified_reason_modified"))
				.setVerified_contact_name(getString(tmp, "verified_contact_name"))
				.setVerified_contact_email(getString(tmp, "verified_contact_email"))
				.setVerified_contact_mobile(getString(tmp, "verified_contact_mobile"))
				.setFollow_me(getBoolean(tmp, "follow_me")).setOnline_status(getInt(tmp, "online_status"))
				.setBi_followers_count(getInt(tmp, "bi_followers_count")).setLang(getString(tmp, "lang"))
				.setStar(getInt(tmp, "star")).setMbtype(getInt(tmp, "mbtype")).setMbrank(getInt(tmp, "mbrank"))
				.setBlock_word(getInt(tmp, "block_word")).setBlock_app(getInt(tmp, "block_app"))
				.setCredit_score(getInt(tmp, "credit_score")).setUser_ability(getInt(tmp, "user_ability"))
				.setUrank(getInt(tmp, "urank")).build();
		return user;
	}

	private static Visible getVisible(SinaDomain sinaDomain, String key) {
		SinaDomain tmp = (SinaDomain) sinaDomain.get(key);
		Visible visible = new Visible(getInt(tmp, "type"), getInt(tmp, "list_id"));
		return visible;
	}

	private static Date dateChange(String date) {
		DateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);
		Date dat;
		try {
			dat = dateFormat.parse(date);
			return dat;
		} catch (ParseException e) {
			logger.error(e.getMessage() + ";解析时间出错，以当前时间为created_date");
			return new Date();
		}
	}
}
