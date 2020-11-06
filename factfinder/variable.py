class Variable:
    def __init__(self, kwargs):
        self.pff_variable = kwargs.get("pff_variable")
        self.census_variable = kwargs.get("census_variable")
        self.domain = kwargs.get("domain")
        self.base_variable = kwargs.get("base_variable")
        self.rounding = kwargs.get("rounding")
        self.source = kwargs.get("source")
        self.meta = kwargs
