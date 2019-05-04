import os

import redis
from satstac import Catalog


HOSTNAME = os.getenv('REDIS_HOSTNAME')
if os.environ['REDIS_PORT']:
    PORT = os.getenv('REDIS_PORT')
else:
    PORT = 9851

class Database(object):

    @staticmethod
    def gen_items(stac_link):
        """
        Generate STAC Items from STAC Catalog entrypoint.
        """
        cat = Catalog.open(stac_link)

        # Check if root
        if cat.id == cat.root():
            for child in cat.children():
                for item in child.items():
                    yield item
        else:
            for item in cat.items():
                yield item

    def __init__(self):
        self.db = redis.Redis(host=HOSTNAME, port=PORT)

    def insert(self, table, stac_item):
        self.db.execute_command('SET', table, stac_item.id, 'bounds', *stac_item.bbox)

    def index_catalog(self, table_name, stac_link):
        for item in self.gen_items(stac_link):
            self.insert(table_name, item)

    def intersects_query(self, table, bbox):
        result = self.db.execute_command('INTERSECTS', table, 'BOUNDS', *bbox)
        return result