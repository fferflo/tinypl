class Item:
    def __init__(self, value):
        self.value = value
        self.annotations = {}

    def __contains__(self, key):
        return key in self.annotations

    def __getitem__(self, key):
        return self.annotations[key]

    def __setitem__(self, key, value):
        self.annotations[key] = value

    def __delitem__(self, key):
        del self.annotations[key]

    @staticmethod
    def unwrap(item_or_value):
        if isinstance(item_or_value, Item):
            return item_or_value.get()
        else:
            return item_or_value

    @staticmethod
    def wrap(item_or_value):
        if isinstance(item_or_value, Item):
            return item_or_value
        else:
            return Item(item_or_value)

    def get(self):
        return self.value

    def set(self, new_value):
        self.value = new_value

class Marker:
    def __init__(self, source):
        self.source = source