class Variable:
    def __init__(self, kwargs):
        self.pff_variable = kwargs.get("pff_variable")
        self.acs_variable = kwargs.get("acs_variable")
        self.domain = kwargs.get("domain")
        self.base_variable = kwargs.get("base_variable")
        self.rounding = kwargs.get("rounding")
        self.source = kwargs.get("source")
        self.median = kwargs.get("median")
        self.range = kwargs.get("range")
        self.design_factor = kwargs.get("design_factor")
        self.meta = kwargs
