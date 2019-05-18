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
        cursor.execute ('SELECT  FROM api_access_info WHERE source = %s AND type = "lending bot"',(exchange,))
        api_info = cursor.fetchone()

        #if api info does not exist throw an exception
        if api_info is None:
            self.log.log_error("API access info not found!")

        api_key, secret = api_info['api_key'], api_info['secret']

        cursor.close()

        return api_key, secret


#Updates the file paths of the images corresponding to a specific image id
def update_path(image_id,image_paths):
    """
    Updates the file paths of the images corresponding to a specific image id

    :param image_id:id of the image whose file paths need to be updated
    :param image_paths:mage_paths contains all the paths including that of original file,thumbnail and transformations
    :return: bool: isSuccessful: outcome of operation
    """

    isSuccessful = False
    try:

        cxn = get_db()
        cursor = cxn.cursor()
        update_path_query = """UPDATE images
                            SET original_path = %s, thumbnail = %s, transformation1_path = %s,
                            transformation2_path = %s, transformation3_path = %s
                            where image_id = %s """
        update_data = (image_paths['original_path'],
                        image_paths['thumbnail_path'],
                        image_paths['transformation1_path'],
                        image_paths['transformation2_path'],
                        image_paths['transformation3_path'],image_id)

        cursor.execute(update_path_query,update_data)

        cxn.commit()
        print("update_path" + str(cursor.rowcount) + "rows affected")

        #if a single row has been updated, the operation was successful
        if cursor.rowcount == 1:
            isSuccessful = True

    except Exception as e:
        # Logs the error appropriately and rollback transaction
        isSuccessful = False
        traceback.print_exc()
        cxn.rollback()


    finally:
        cursor.close()

    #return outcome of the operation
    return isSuccessful


    def __del__(self):
      # body of destructor
      self.connection.close()
