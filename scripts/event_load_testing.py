__author__ = 'johnbaker'


from locust import HttpLocust, TaskSet


def login(self):
    """Authenticate the user.

    Arguments:
        email (unicode): The user's email address.
        password (unicode): The user's password.
        remember (boolean): Whether "remember me" is checked.

    """
    params = {
        'event_type' : 'show_transcript',
        'event' : {"id":"i4x-BerkeleyX-Stat2_1x-video-58424ad2f75048798b4480aa699cc215","currentTime":0,"code":"iOOYGgLADj8"}

    }
    self.post("/events", params)


def index(l):
    #l.client.get("/")
    #l.client.post("/events")
    pass

def profile(l):
    # l.client.get("/courses/BerkeleyX/Stat2.1x/2013_Spring/info")
    #l.client.get("/modelflow/last")
    pass

class UserBehavior(TaskSet):
    tasks = {index:2, profile:1}

    def on_start(self):
        login(self)



class WebsiteUser(HttpLocust):
    task_set = UserBehavior
    min_wait=5000
    max_wait=9000