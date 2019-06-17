import pytest
import cx_Oracle
import pandas as pd
import utils
import os
from AESCipher import AESCipher
import extractionDetails


class TestExtraction:
    def test_oracle_connection(self):
        connection = utils.get_oracle_connection_obj()
        assert type(connection) == cx_Oracle.Connection, "Username or password or connection string is incorrect for " \
                                                         "DLEXD database mentioned in application.properties file"

    def test_oracle_table(self):
        connection = utils.get_oracle_connection_obj()
        config = utils.get_config_parser()

        tbl_list = (config.get('Database', 'extraction_tablename'), config.get('Database', 'connection_tablename'))
        dbcur = connection.cursor()
        try:
            for tbl in tbl_list:
                dbcur.execute("SELECT * FROM {}".format(tbl))
        except Exception as ex:
            err, = ex.args
            pytest.fail(err.message)

    def test_password_file(self):
        connection = utils.get_oracle_connection_obj()
        config = utils.get_config_parser()
        query = f"SELECT PASSWORD_FILE FROM {config.get('Database', 'connection_tablename')}"
        pwrd_df = pd.read_sql(query, con=connection)
        for index, row in pwrd_df.iterrows():
            exists = os.path.isfile(row['PASSWORD_FILE'])
            assert exists, f"{row['PASSWORD_FILE']} file does not exists "

    def test_encrypt_decrypt(self):
        config = utils.get_config_parser()
        enc = AESCipher(config.get('AES', 'key'))
        f = open("password.txt", "w+")
        f.write("Password")
        f.close()
        enc.encrypt_file("password.txt", out_filename="password.enc")
        password = enc.decrypt_file("password.enc")
        assert password == "Password", "Failed to decrypt file"

    def test_source_db_connection(self):
        connection = utils.get_oracle_connection_obj()
        config = utils.get_config_parser()
        enc = AESCipher('[EX\xc8\xd5\xbfI{\xa2$\x05(\xd5\x18\xbf\xc0\x85)\x10nc\x94\x02)j\xdf\xcb\xc4\x94\x9d(\x9e')
        query = f"SELECT CONNECTION_STRING,USERNAME,PASSWORD_FILE FROM {config.get('Database', 'connection_tablename')}"
        source_db_df = pd.read_sql(query, con=connection)
        for index, row in source_db_df.iterrows():
            password = enc.decrypt_file(row['PASSWORD_FILE'])
            connection = utils.get_oracle_connection_obj(row['USERNAME'], password, row['CONNECTION_STRING'])
            assert type(connection) == cx_Oracle.Connection, "Username or password or connection string is incorrect"

    def test_source_tables(self):
        connection = utils.get_oracle_connection_obj()
        config = utils.get_config_parser()
        enc = AESCipher('[EX\xc8\xd5\xbfI{\xa2$\x05(\xd5\x18\xbf\xc0\x85)\x10nc\x94\x02)j\xdf\xcb\xc4\x94\x9d(\x9e')
        query = f"SELECT CONNECTION_STRING,USERNAME,PASSWORD_FILE,DATABASE_NAME,TABLE_NAME FROM " \
            f"{config.get('Database', 'extraction_tablename')} E JOIN " \
            f"{config.get('Database', 'connection_tablename')} C ON E.CONNECTION_ID=C.CONNECTION_ID"
        join_df = pd.read_sql(query, con=connection)
        for index, row in join_df.iterrows():
            password = enc.decrypt_file(row['PASSWORD_FILE'])
            connection = utils.get_oracle_connection_obj(row['USERNAME'], password, row['CONNECTION_STRING'])
            dbcur = connection.cursor()
            try:
                dbcur.execute(f"SELECT * FROM {row['DATABASE_NAME']}.{row['TABLE_NAME']}")
            except Exception as ex:
                err, = ex.args
                pytest.fail(err.message)

    def test_get_select_query(self):
        query = extractionDetails.get_select_query("*", "database", "table", "id=0")
        assert query == "SELECT * FROM database.table WHERE id=0", "Error in get_select_query function"
