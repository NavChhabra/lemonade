import os


def get_env():
    """
    For setting up variables which are commonly being used at many places throughout the code. Saving them in dict
    format.
    return :
        dictionary of set variables
    """
    os.chdir("../")
    BATCH_HOME = os.getcwd()

    env_dict = {
        "BATCH_HOME": BATCH_HOME,
        "EVENTS_CHECKPOINT": BATCH_HOME + "/checkpoint/events/",
        "STATUS_CHECKPOINT": BATCH_HOME + "/checkpoint/status/",
        "EVENTS_FILENAME": "vehicles_events",
        "STATUS_FILENAME": "vehicles_status",
        "EVENTS_TABLE": "vehicle_events",
        "STATUS_TABLE": "vehicle_status",
        "DAILY_LOAD_TABLE": "vehicle_daily_summarized"
    }

    return env_dict
