package zx.soft.weibo.mapred.activeweibo;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.redis.client.cache.Cache;
import zx.soft.redis.client.cache.CacheFactory;
import zx.soft.utils.http.ClientDao;
import zx.soft.utils.http.HttpClientDaoImpl;
import zx.soft.weibo.mapred.utils.Constant;
import zx.soft.weibo.sina.common.JsonNodeUtils;

public class ActiveUserStorage {

	public static Logger logger = LoggerFactory.getLogger(ActiveUserStorage.class);

	public static final String ACTIVE_USER = "sent:sina:activeUser";

	public static List<String> getActiveUserId(int from) {
		ClientDao clientDao = new HttpClientDaoImpl();
		String jsonStr = clientDao.doGet(Constant.USER_SCORE_GET + from);
		return parseJsonTreeIDs(jsonStr);
	}

	private static List<String> parseJsonTreeIDs(String jsonStr) {
		List<String> result = new ArrayList<>();
		JsonNode node = JsonNodeUtils.getJsonNode(jsonStr);
		for (JsonNode no : node) {
			result.add(no.toString().substring(1, no.toString().length() - 1));
		}
		logger.info(result.size() + "");
		return result;
	}

	public static void main(String[] args) {
		Cache cache = CacheFactory.getInstance();
		for (int i = 0; i < 1000; i++) {
			List<String> users = ActiveUserStorage.getActiveUserId(i * 1000);
			String[] strarray = users.toArray(new String[0]);
			cache.sadd(ACTIVE_USER, strarray);
		}
	}
}
