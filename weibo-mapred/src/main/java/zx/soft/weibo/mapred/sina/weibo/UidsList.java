package zx.soft.weibo.mapred.sina.weibo;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import zx.soft.redis.client.cache.Cache;
import zx.soft.redis.client.cache.CacheFactory;

public class UidsList {

	private static Cache cache = CacheFactory.getInstance();

	public static final String CLOSE_USERS_KEY = "sent:sina:closeUsers";

	public static final String PROCESSED_USERS_KEY = "sent:sina:processedUsers";//689107

	public static final String WAIT_USERS_KEY = "sent:sina:waitUsers";//11717933

	private static List<String> getUids() {

		Set<String> set = cache.smembers(PROCESSED_USERS_KEY);
		List<String> uids = new ArrayList<>();
		Iterator<String> iterator = set.iterator();
		while (iterator.hasNext()) {
			uids.add(iterator.next().toString());
		}
		return uids;
	}

	public static List<List<String>> getSubList() {
		List<List<String>> subLists = new ArrayList<>();
		List<String> uids = new ArrayList<>();
		uids = getUids();
		List<String> subUids = null;
		for (int i = 0; i < uids.size(); i++) {
			if (i % 1000 == 0) {
				subUids = new ArrayList<>();
				subUids.add(uids.get(i));
			} else {
				subUids.add(uids.get(i));
				if (i % 1000 == 999) {
					subLists.add(subUids);
				}
			}
		}
		return subLists;
	}

	public static void main(String[] args) {
		List<List<String>> subLists = UidsList.getSubList();
		for (int i = 0; i < subLists.size(); i++) {
			System.out.println(subLists.get(i));
		}
	}
}
