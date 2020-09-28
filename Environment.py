import os
LOCAL_ENV = os.path.isfile('../local.ini')
if LOCAL_ENV:
    import configparser
    config = configparser.ConfigParser()
    config.read('../local.ini')
    STAGE = config['env']['stage']
