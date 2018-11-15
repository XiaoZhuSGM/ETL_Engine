from celery import Celery as _Celery


class Celery(_Celery):
    def __init__(self, app=None):
        if app:
            self.init_app(app)

    def init_app(self, app):
        TaskBase = self.Task

        class ContextTask(TaskBase):
            def __call__(self, *args, **kwargs):
                with app.app_context():
                    return self.run(*args, **kwargs)

        print(app.config["CELERY_BROKER_URL"])
        print(app.config["CELERY_BACKEND_URL"])
        super().__init__(
            app.import_name,
            broker=app.config["CELERY_BROKER_URL"],
            backend=app.config["CELERY_BACKEND_URL"],
        )
        # self.conf.update(app.config)
        self.Task = ContextTask
