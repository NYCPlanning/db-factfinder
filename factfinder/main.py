from census import Census

class pff():
    def __init__(self, api_key):
        self.c = Census(api_key)
    