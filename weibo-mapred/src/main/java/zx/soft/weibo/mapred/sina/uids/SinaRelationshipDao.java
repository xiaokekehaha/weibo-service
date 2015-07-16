package zx.soft.weibo.mapred.sina.uids;

import zx.soft.weibo.mapred.domain.UsersAndIds;
import zx.soft.weibo.mapred.utils.RequestLimitException;

public interface SinaRelationshipDao {

	UsersAndIds getFollowers(String uid) throws RequestLimitException;

	UsersAndIds getFriends(String uid) throws RequestLimitException;

}
