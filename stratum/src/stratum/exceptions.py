class DictKeyError(KeyError):
    def __init__(self, key, dict_) -> None:
        super().__init__(key)
        self.key = key
        self.dict_ = dict_

    def __str__(self) -> str:
        found_keys = ", ".join(repr(k) for k in self.dict_.keys())
        return f"Could not find key '{self.key}', available keys: [{found_keys}]"
