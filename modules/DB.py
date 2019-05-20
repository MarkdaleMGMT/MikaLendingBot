import mysql.connector

class DB():

    def __init__(self, cfg, log):
        self.cfg = cfg
        self.log = log
        self.connection = self.connect_to_database(self.cfg)

    def connect_to_database(self, cfg):
        """
            Reads parameters from configuration and connects to the db
        :return: connection object
        """
        return mysql.connector.connect(user=cfg.get("DATABASE", "username", None),
                                   password=cfg.get("DATABASE", "password", None),
                                   host=cfg.get("DATABASE", "host", None),
                                   database=cfg.get("DATABASE", "db_name", None),
                                   auth_plugin='mysql_native_password')

        self.log.log("db connection successful")

    def get_api_access_info(self,exchange):
        exchange = exchange.lower()
        cursor = self.connection.cursor(buffered=True)

        #fetches a record for user corresponding to a specific user_name
        cursor.execute ('SELECT api_key, secret FROM api_access_info WHERE description = %s AND type = %s',(exchange,'lending bot'))
        api_info = cursor.fetchone()

        #if api info does not exist throw an exception
        if api_info is None:
            self.log.log_error("API access info not found!")

        api_key, secret = str(api_info[0]), str(api_info[1])
       	cursor.close()

        return api_key, secret


    def __del__(self):
      # body of destructor
      self.connection.close()
