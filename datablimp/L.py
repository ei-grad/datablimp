from datablimp.base import Base


class Base(Base):
    _process_method_name = 'load'


class AppendTo(Base):

    def __init__(self, output):
        super().__init__()
        self.output = output

    def load(self, data):
        self.output.append(data)
