from handlers.common import CommonHandler


class TaskHandler(CommonHandler):
    def __init__(self, xapi):
        super().__init__(xapi.task)

    def cancel(self, task_ref):
        self.xapi.cancel(task_ref)

    def get_status(self, task_ref):
        self.xapi.get_status(task_ref)
