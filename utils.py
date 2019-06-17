import configparser
import cx_Oracle


def get_config_parser():
    """
    Create configuration object for application.properties file
    :return: config object
    """
    config = configparser.RawConfigParser()
    config.read('application.properties')
    return config


def get_oracle_connection_obj(username=None, password=None, connection_str=None):
    """
    Create database connection object,
    if arguments are none then read corresponding arguments from properties file
    :param username: database username
    :param password: password
    :param connection_str: complete connection string
                          eg: localhoast:1521/servicename
    :return: connection object
    """
    config = get_config_parser()
    if username is not None and password is not None and connection_str is not None:
        url = username + "/" + password + "@" + connection_str
    else:
        url = config.get('Database', 'username') + "/" + config.get('Database', 'password') + "@" + \
              config.get('Database', 'hostname') + "/" + config.get('Database', 'service')
    connection = cx_Oracle.Connection(url)
    return connection






