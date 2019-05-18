import mysql.connector

class DB():

    def __init__(self, cfg, log):
        self.cfg = cfg
        self.log = log
        self.connection = connect_to_database(self.cfg)

    def connect_to_database(cfg):
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

    def __del__(self):
      # body of destructor
      self.connection.close()
