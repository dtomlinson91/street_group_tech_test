import apache_beam as beam


class DebugShowEmptyColumn(beam.DoFn):
    def __init__(self, index):
        self.index = index

    def process(self, element):
        column = element[self.index]
        if len(column) == 0:
            yield element
        return None


class DebugShowColumnWithValueIn(beam.DoFn):
    def __init__(self, index, value):
        self.index = index
        self.value = value

    def process(self, element):
        column = element[self.index]
        if self.value in column:
            yield element
        return None


class DebugPrint(beam.DoFn):
    def process(self, element):
        print(element)
        yield element
