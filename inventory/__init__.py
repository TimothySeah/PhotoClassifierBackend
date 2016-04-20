from django.apps import AppConfig
from multiprocessing import Lock

# startup code
class MyAppConfig(AppConfig):
    name = 'inventory'
    verbose_name = 'Inventory'
    classifier_lock = Lock()

    def ready(self):
        print("APP CONFIG HERE")


default_app_config = 'inventory.__init__.MyAppConfig'