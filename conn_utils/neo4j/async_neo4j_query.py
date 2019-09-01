# coding:utf-8
from system_config import system_config
from aioneo4j import Neo4j
from risk_platform.neo4j.hydrate import hydrate

# old id + old ph
host = system_config.get_neo4j_host()
username = system_config.get_neo4j_username()
pw = system_config.get_neo4j_password()
graph_url = 'http://%s:%s@%s' % (username, pw, host)

# vn
vn_host = system_config.get_vn_neo4j_host()
vn_username = system_config.get_vn_neo4j_username()
vn_pw = system_config.get_vn_neo4j_password()
vn_graph_url = 'http://%s:%s@%s' % (vn_username, vn_pw, vn_host)

# new id
id_host = system_config.get_id_neo4j_new_host()
id_username = system_config.get_id_neo4j_new_username()
id_pw = system_config.get_id_neo4j_new_password()
id_graph_url = 'http://%s:%s@%s' % (id_username, id_pw, id_host)

# new ph
ph_host = system_config.get_ph_neo4j_new_host()
ph_username = system_config.get_ph_neo4j_new_username()
ph_password = system_config.get_ph_neo4j_new_password()
ph_graph_url = 'http://%s:%s@%s' % (ph_username, ph_password, ph_host)


async def query(statement):
    async with Neo4j(graph_url) as conn:
        """simple hydrate a dictionary of data base on py2neo2.0  """
        data = await conn.cypher(statement)
        # logging.info("match %s data:%s", statement, data)
        return hydrate(data)


async def vn_query(statement):
    async with Neo4j(vn_graph_url) as conn:
        data = await conn.cypher(statement)
        return hydrate(data)


async def id_query(statement):
    async with Neo4j(id_graph_url) as conn:
        data = await conn.cypher(statement)
        return hydrate(data)


async def ph_query(statement):
    async with Neo4j(ph_graph_url) as conn:
        data = await conn.cypher(statement)
        return hydrate(data)


# async_conn def risk_user_strong_relation_cnt():
#     uid = 14652688
#     match_sql = '''match p = (n:uid {name: '%s'})-[:self_number]-(m)-[:call_history|address_book|relative_phone_number]-(l:uid) return distinct l.name as uid''' % uid
#     result = await query(match_sql)
#     print(result)
#     print(hydrate(result))
#     print(list(map(lambda one: (one["uid"]), hydrate(result))))
#     # print(result[0]["cnt"])
#     # return result[0]["cnt"]
#
# if __name__ == "__main__":
#
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(risk_user_strong_relation_cnt())
#     loop.close()


Neo4jClient = {
    1: id_query,
    3: ph_query,
    4: vn_query
}
