#include "common.h"

static int m_command(lua_State *L) {
	mongoc_client_t *client = checkPooledClient(L, 1);
	const char *dbname = luaL_checkstring(L, 2);
	bson_t *command = castBSON(L, 3);
	bson_t *options = toBSON(L, 4);
	mongoc_read_prefs_t *prefs = toReadPrefs(L, 5);
	bson_t reply;
	bson_error_t error;
	bool status = mongoc_client_command_with_opts(client, dbname, command, prefs, options, &reply, &error);
	if (!bson_has_field(&reply, "cursor")) return commandReply(L, status, &reply, &error);
	pushCursor(L, mongoc_cursor_new_from_command_reply_with_opts(client, &reply, options), 1);
	return 1;
}

static int m_getCollection(lua_State *L) {
	mongoc_client_t *client = checkPooledClient(L, 1);
	const char *dbname = luaL_checkstring(L, 2);
	const char *collname = luaL_checkstring(L, 3);
	pushCollection(L, mongoc_client_get_collection(client, dbname, collname), false, 1);
	return 1;
}

static int m_getDatabase(lua_State *L) {
	mongoc_client_t *client = checkPooledClient(L, 1);
	const char *dbname = luaL_checkstring(L, 2);
	pushDatabase(L, mongoc_client_get_database(client, dbname), 1);
	return 1;
}

static int m_getDatabaseNames(lua_State *L) {
	mongoc_client_t *client = checkPooledClient(L, 1);
	bson_t *options = toBSON(L, 2);
	bson_error_t error;
	return commandStrVec(L, mongoc_client_get_database_names_with_opts(client, options, &error), &error);
}

static int m_getDefaultDatabase(lua_State *L) {
	mongoc_database_t *database = mongoc_client_get_default_database(checkPooledClient(L, 1));
	luaL_argcheck(L, database, 1, "default database is not configured");
	pushDatabase(L, database, 1);
	return 1;
}

static int m_getGridFS(lua_State *L) {
	mongoc_client_t *client = checkPooledClient(L, 1);
	const char *dbname = luaL_checkstring(L, 2);
	const char *prefix = lua_tostring(L, 3);
	bson_error_t error;
	mongoc_gridfs_t *gridfs = mongoc_client_get_gridfs(client, dbname, prefix, &error);
	if (!gridfs) return commandError(L, &error);
	pushGridFS(L, gridfs, 1);
	return 1;
}

static int m_getReadPrefs(lua_State *L) {
	pushReadPrefs(L, mongoc_client_get_read_prefs(checkPooledClient(L, 1)));
	return 1;
}
static int m_setReadPrefs(lua_State *L) {
	mongoc_client_t *client = checkPooledClient(L, 1);
	mongoc_read_prefs_t *prefs = checkReadPrefs(L, 2);
	mongoc_client_set_read_prefs(client, prefs);
	return 0;
}

static const luaL_Reg pooled_client_funcs[] = {
	{"command", m_command},
	{"getCollection", m_getCollection},
	{"getDatabase", m_getDatabase},
	{"getDatabaseNames", m_getDatabaseNames},
	{"getDefaultDatabase", m_getDefaultDatabase},
	{"getGridFS", m_getGridFS},
	{"getReadPrefs", m_getReadPrefs},
	{"setReadPrefs", m_setReadPrefs},
	{0, 0}
};

int newPooledClient(lua_State *L) {
	mongoc_client_t *client = lua_touserdata(L, 1);
	if(client) {
		pushHandle(L, client, 0, 0);
		setType(L, TYPE_POOLED_CLIENT, funcs);
		return 1;
	}
	return 0;
}

mongoc_client_t *checkPooledClient(lua_State *L, int idx) {
	return *(mongoc_client_t **)luaL_checkudata(L, idx, TYPE_POOLED_CLIENT);
}

static int m_client_pool_try_pop(lua_State *L) {
	mongoc_client_pool_t *client_pool = checkClientPool(L, 1);
	mongoc_client_t *client = mongoc_client_pool_try_pop(client_pool);
	if(client) {
		lua_pushlightuserdata(L, client);
		return 1;
	}
	return 0;
}

static int m_client_pool_pop(lua_State *L) {
	mongoc_client_pool_t *client_pool = checkClientPool(L, 1);
	mongoc_client_t *client = mongoc_client_pool_pop(client_pool);
	if(client) {
		lua_pushlightuserdata(L, client);
		return 1;
	}
	return 0;
}

static int m_client_pool_push(lua_State *L) {
	mongoc_client_pool_t *client_pool = checkClientPool(L, 1);
	mongoc_client_t *client = lua_touserdata(L, 2);
	if(client) {
		mongoc_client_pool_push(client_pool, client);
	}
	return 0;
}

static int m_client_pool_destroy(lua_State *L) {
	mongoc_client_pool_destroy(checkClientPool(L, 1));
	unsetType(L);
	return 0;
}

static const luaL_Reg client_pool_funcs[] = {
	{"tryPop", m_client_pool_try_pop},
	{"pop", m_client_pool_pop},
	{"push", m_client_pool_push},
	{"__gc", m_client_pool_destroy},
	{0, 0}
};

int newClientPool(lua_State *L) {
	bson_error_t error;
	mongoc_uri_t *uri = mongoc_uri_new_with_error(luaL_checkstring(L, 1), &error);
	if(!uri) {
		lua_pushnil(L);
		lua_pushstring(L, error.message);
      	return 2;
	}
	mongoc_client_pool_t *client_pool = mongoc_client_pool_new(uri);
	mongoc_uri_destroy(uri);
	luaL_argcheck(L, client_pool, 1, "invalid format");
	pushHandle(L, client_pool, 0, 0);
	setType(L, TYPE_CLIENT_POOL, client_pool_funcs);
	return 1;
}

mongoc_client_pool_t *checkClientPool(lua_State *L, int idx) {
	return *(mongoc_client_pool_t **)luaL_checkudata(L, idx, TYPE_CLIENT_POOL);
}
