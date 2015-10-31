from pyspark import SparkContext, SparkConf
import adp_cube.Util as adpu


class Calculator:
    def __init__(self, data=None, metrics=None, combo=None, index=None):
        self.data = data
        self.metrics = None
        self.combo = combo
        self.index = index

    def min(self):
        return self.data \
            .map(lambda (key, value): (adpu.build_key(self.combo, key), value)) \
            .reduceByKey(lambda a, b: a if a < b else b)

    def max(self):
        return self.data \
            .map(lambda (key, value): (adpu.build_key(self.combo, key), value)) \
            .reduceByKey(lambda a, b: a if a > b else b)

    def avg_with_cnt(self):
        return self.data \
            .map(lambda (key, value): (adpu.build_key(self.combo, key), value)) \
            .combineByKey(lambda value: (value, 1),
                          lambda x, value: (x[0] + value, x[1] + 1),
                          lambda x, y: (x[0] + y[0], x[1] + y[1])) \
            .map(lambda (label, (value_sum, count)):
                 (label, str(count) + "," + str(value_sum / count)))

    def sum(self):
        return self.data \
            .map(lambda line: (",".join(line.strip().split(",")[:5]),
                               float(line.strip().split(",")[self.index]))) \
            .reduceByKey(lambda a, b: a + b)


def exec_build(data, args):
    # Get all the arguments from the parser
    combo_file = args['combo']
    qtr = args['quarter']
    output = args['output']
    target = args['target']

        # Find all months within 12 months
    all_months = adpu.get_12_months(qtr)

    all_combos = adpu.read_combo_file(combo_file)
    # TODO MIGHT BE REMOVED IF THE INPUT IS FROM HIVE TABLE
    header = data.first()

    # TODO NEED TO BE MODIFIED IF INPUT IS FROM HIVE TABLE
    idx = header.strip().split(",").index(target)

    # Apply filters, months, states
    # TODO CREATE A FUNCTION TO APPLY A LIST OF FILTERS
    # TODO STATUS == 'A' OR STATUS == 'T'
    # TODO AND JOB_SCORE > 70.0
    # TODO AND ((RATE_TYPE == 'H' AND RATE_AMOUNT <500) OR (RATE_TYPE == 'S' AND RATE_AMOUNT < 40000))
    data = data.filter(lambda x : x != header) \
        .filter(lambda line: line.strip().split(",")[-1] in all_months) \
        .filter(lambda line: line.strip().split(",")[3] in adpu.all_states)
        #.filter(lambda line: line.strip().split(",")[?]) in ['T','A'])

    # Compute the total wage for each person within last 12 months
    cal = Calculator()
    cal.data = data
    cal.index = idx
    person_total = cal.sum()
    # person_total = calculate_sum(data, idx)

    # Iterate all combos
    for combo in all_combos:
        # Computer average
        calculator = Calculator(data = person_total, combo = combo)
        data_avg = calculator.avg_with_cnt()
        # data_avg = calculate_avg_with_cnt(person_total, combo)

        # Apply filter employee count > 180
        data_new = data_avg.filter(lambda (x,y) : int(y.split(",")[0])>180)

        # Computer min
        data_min = calculator.min()
        # data_min = calculate_min(person_total, combo)

        # Computer max
        data_max = calculator.max()
        # data_max = calculate_max(person_total, combo)

        data_final = data_new.join(data_min).join(data_max) \
            .map(lambda (key, value) : ",".join(key.split(",") +
                [i.strip("'()") for i in str(value).split(",")])) \
            .repartition(1)

        for i in data_final.collect():
            print i

        output_path = output + combo.split(",")[0]
        data_final.saveAsTextFile(output_path)

def main():
    # Build the arguments parser and parse the arguments
    args = adpu.parse_arguments()
    data_file = args['input']
    master = args['master']

    conf = SparkConf() \
        .setAppName('Cube Build Beta') \
        .setMaster(master) \
        .set("spark.hadoop.validateOutputSpecs", "false") #TODO TEST PURPOSE, WILL BE REMOVED

    sc = SparkContext(conf=conf)
    data = sc.textFile(data_file)
    exec_build(data, args)


if __name__ == '__main__':
    main()
