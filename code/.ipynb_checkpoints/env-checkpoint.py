import os


def get_env():

    #CURRENT_DIR=os.system('pwd')
    os.chdir("../")
    BATCH_HOME = os.getcwd()

    env_dict = {
        "BATCH_HOME": BATCH_HOME,
        "EVENTS_CHECKPOINT": BATCH_HOME + "/checkpoint/events/",
        "STATUS_CHECKPOINT": BATCH_HOME + "/checkpoint/status/",
        "EVENTS_FILENAME": "vehicles_events",
        "STATUS_FILENAME": "vehicles_status"
    }

    return env_dict
