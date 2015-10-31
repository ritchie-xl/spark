import adp_cube.Util

class Calculator:
    def __init__(self, data=None, metrics=None, combo=None, index=None):
        self.data = data
        self.metrics = None
        self.combo = combo
        self.index = index

    def min(self):
        ret_val =  self.data \
            .map(lambda (key, value): (adp_cube.build_key(self.combo, key), value)) \
            .reduceByKey(lambda a, b: a if a < b else b)
        return ret_val

    def max(self):
        ret_val = self.data \
            .map(lambda (key, value): (adp_cube.build_key(self.combo, key), value)) \
            .reduceByKey(lambda a, b: a if a > b else b)
        return ret_val

    def avg_with_cnt(self):
        ret_val = self.data \
            .map(lambda (key, value): (adp_cube.build_key(self.combo, key), value)) \
            .combineByKey(lambda value: (value, 1),
                          lambda x, value: (x[0] + value, x[1] + 1),
                          lambda x, y: (x[0] + y[0], x[1] + y[1])) \
            .map(lambda (label, (value_sum, count)):
                 (label, str(count) + "," + str(value_sum / count)))
        return ret_val

    def sum(self):
        ret_val = self.data \
            .map(lambda line: (",".join(line.strip().split(",")[:5]),
                               float(line.strip().split(",")[self.index]))) \
            .reduceByKey(lambda a, b: a + b)
        return ret_val
