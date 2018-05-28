from json import JSONEncoder


class DateTimeEncoder(JSONEncoder):
    def default(self, datetime):
        return datetime.value
