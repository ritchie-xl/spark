#!/usr/bin/python
import logging
import ConfigParser
import sys

from pyspark import SparkConf,SparkContext

__DEBUG__ = True

# TODO(xial): Add more metric calculation if needed here
class Calculator:
    def __init__(self, metrics=None, combo=None, index=None):
        self.metrics = None
        self.combo = combo
        self.index = index

    def min(self,data):
        """Return a RDD
        Compute the minimum for the input RDD's specified column
        """
        return data \
            .map(lambda (key, value): (build_key(self.combo, key), value)) \
            .reduceByKey(lambda a, b: a if a < b else b)

    def max(self,data):
        """Return a RDD
        Compute the maximum for the input RDD's specified column
        """
        return data \
            .map(lambda (key, value): (build_key(self.combo, key), value)) \
            .reduceByKey(lambda a, b: a if a > b else b)

    def avg_with_cnt(self,data):
        return data \
            .map(lambda (key, value): (build_key(self.combo, key), value)) \
            .combineByKey(lambda value: (value, 1),
                          lambda x, value: (x[0]+value, x[1]+1),
                          lambda x, y: (x[0]+y[0], x[1]+y[1])) \
            .map(lambda (label, (value_sum, count)):
                 (label, str(count) + "," + str(value_sum/count)))

    def sum(self,data):
        return data \
            .map(lambda line: (",".join(line.strip().split(",")[:5]),
                               float(line.strip().split(",")[self.index]))) \
            .reduceByKey(lambda a, b: a + b)

all_states = ['AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL',
            'IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE',
            'NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD',
            'TN','TX','UT','VT','VA','WA','WV','WI','WY']


# Get all 12 months within the input quarter
def get_12_months(qtr):
    """
    :param qtr: The quarter(YYYYQQ, eg, 2015Q2)
    :return: A list with 12 months including the input quarter and previous 3 quarters
    """
    ret_val = list()
    yyyy, mm = int(qtr[:4]), int(qtr[-1])*3

    for i in range(12):
        if mm == 0:
            yyyy -= 1
            mm = 12
        new_yyyymm = str(yyyy) + '0' + str(mm) if mm < 10 else str(yyyy) + str(mm)
        ret_val.append(new_yyyymm)
        mm -= 1
    return ret_val


def build_key(combo, line):
    """
    :param combo: A binary array indicate which dimensions are included in the calculation
    :param line: The actual data
    :return: A line include the data with only specified dimensions, otherwise empty
    """
    fields = line.split(",")
    combos = combo.strip().split(",")
    f_list = list()
    for i in range(1, len(fields)):
        if combos[i-1] != '0':
            f_list.append(fields[i])
        else:
            f_list.append("")
    key = ",".join(f_list)
    return key


# def calculate_min(data, combo):
#     return data \
#         .map(lambda (key,value) : (build_key(combo,key), value)) \
#         .reduceByKey(lambda a,b : a if a < b else b)


# def calculate_max(data, combo):
#     return data \
#         .map(lambda (key,value) : (build_key(combo,key), value)) \
#         .reduceByKey(lambda a,b : a if a > b else b)


# def calculate_avg_with_cnt(data, combo):
#     return data \
#         .map(lambda (key,value):(build_key(combo,key),value)) \
#         .combineByKey(lambda value: (value, 1),
#                     lambda x, value: (x[0] + value, x[1] + 1),
#                     lambda x, y: (x[0] + y[0], x[1] + y[1])) \
#         .map(lambda (label, (value_sum, count)) :
#                                     (label , str(count) + "," + str(value_sum/count)))


# def calculate_sum(data, col_idx):
#     return data\
#         .map(lambda line: (",".join(line.strip().split(",")[:5]), float(line.strip().split(",")[col_idx])))\
#         .reduceByKey(lambda a, b:a+b)


# TODO(xial): Design the way to apply filter to make it generic
def apply_filter(data, filter):
    """
    :param data: The RDD will be applied filter on
    :param filter: The filter string
    :return: An new RDD has been applied the input filter
    """
    return 0


def read_config(config_file):
    """
    :param config_file: Input config file path, the config file is in java properties file format
    :return: A dictionary include all properties and their values
    """
    logging.info("Reading configuration from %s ..." % config_file)
    conf = dict()
    parser = ConfigParser.RawConfigParser()
    parser.read(config_file)

    conf['input'] = parser.get('DataSpecs', 'input')
    conf['target'] = parser.get('DataSpecs', 'target')
    conf['quarter'] = parser.get('DataSpecs','quarter')
    conf['master'] = parser.get('SparkSpecs','master')

    # Add '/' if out path doesn't ends with /
    out = parser.get('DataSpecs', 'output')
    out = out if out[-1] == '/' else out + '/'
    conf['output'] = out
    # Process the combos
    items = parser.items('Combos')
    all_dims = parser.get('DataSpecs','all_dimensions').split(",")

    levels = dict((int(x[-1]),y) for x,y in items if y !='')
    dim_idx = dict((y,x) for x,y in enumerate(all_dims))

    combos_tmp = list()
    for level in levels.keys():
        if level == 1:
            combos_tmp += levels[level].split(",")
        else:
            combos_tmp += [i.split(",") for i in levels[level].strip("[]'").split("],[")]

    combos = dict()

    for combo in combos_tmp:
        tmp = ['0']*4
        if type(combo) == str:
            tmp[dim_idx[combo]] = '1'
            combo_name = combo
        else:
            for field in combo:
                if field in all_dims:
                    tmp[dim_idx[field]] = '1'
                combo_name = " and ".join(combo)
        combos[",".join(tmp)] = combo_name
    conf['all_combos'] = combos

    logging.info('Read the configuration file Successfully!')

    return conf


def exec_build(data, config):
    """
    :param data: The RDD will run all the calculation on
    :param config: Config file path
    :return: None
    """

    # Get all the arguments from the parser
    qtr = config['quarter']
    output = config['output']
    target = config['target']
    all_combos_dict = config['all_combos']
    all_combos = all_combos_dict.keys()

    # Find all months within 12 months
    all_months = get_12_months(qtr)
    logging.info("Cube will be built base on following months: %s" % str(all_months))

    # TODO(xial): MIGHT BE REMOVED IF THE INPUT IS FROM HIVE TABLE
    header = data.first()

    # TODO(xial): NEED TO BE MODIFIED IF INPUT IS FROM HIVE TABLE
    idx = header.strip().split(",").index(target) # Benchmark target variable's column index in data

    # Apply filters, months, states
    # TODO(xial): CREATE A FUNCTION TO APPLY A LIST OF FILTERS
    # TODO(xial): STATUS == 'A' OR STATUS == 'T'
    # TODO(xial): AND JOB_SCORE > 70.0
    # TODO(xial): AND ((RATE_TYPE == 'H' AND RATE_AMOUNT <500) OR (RATE_TYPE == 'S' AND RATE_AMOUNT < 40000))
    logging.info("Applying filter on months and states...")
    data = data.filter(lambda x : x != header) \
        .filter(lambda line: line.strip().split(",")[-1] in all_months) \
        .filter(lambda line: line.strip().split(",")[3] in all_states)
        #.filter(lambda line: line.strip().split(",")[?]) in ['T','A'])

    # Compute the total wage for each person within last 12 months
    logging.info("Calculating total wage for each person in previous 12 months ...")
    person_total = Calculator(index=idx).sum(data=data)
    person_total.cache()

    # Iterate all combos
    for combo in all_combos:
        logging.info("Processing combo: %s ..." % all_combos_dict[combo])
        # Computer average
        calculator = Calculator(combo=combo)
        data_avg = calculator.avg_with_cnt(data=person_total)
        # data_avg = calculate_avg_with_cnt(person_total, combo)

        # Apply filter employee count > 180
        logging.info("Applying filter employee count > 5")
        data_new = data_avg.filter(lambda (x,y) : int(y.split(",")[0])>5)

        # Computer min
        data_min = calculator.min(data=person_total)
        # data_min = calculate_min(person_total, combo)

        # Computer max
        data_max = calculator.max(data=person_total)
        # data_max = calculate_max(person_total, combo)

        data_final = data_new.join(data_min).join(data_max) \
            .map(lambda (key, value) : ",".join(key.split(",") +
                [i.strip("'()") for i in str(value).split(",")])) \
            .repartition(1)

        logging.info("Successfully build cube for combo: " + all_combos_dict[combo])
        if __DEBUG__:
            logging.info("The result for combo: %s " % all_combos_dict[combo])
            for i in data_final.collect():
                print i

        output_path = output + qtr + '/' + str(int(''.join([i for i in combo.split(",")]), 2))
        logging.info("Saving result to: %s ..." % output_path)
        data_final.saveAsTextFile(output_path)


def main():
    """
    Entrance of the script
    :return:None
    """
    # Initial the logger
    logging.basicConfig(level=logging.INFO) #, filename=str(round(time.time()*1000))+'.txt')
    logger = logging.getLogger('benchmark_cube_build')

    # Get config file
    config_file=sys.argv[1]

    # Read config file
    config = read_config(config_file)

    input_path = config['input']
    master = config['master']

    logger.info('The cube will be built on Spark Master: %s' % master)
    logger.info('Start Spark Instance ...')
    logging.info('AppName is Cube Build Beta')

    # Start Spark
    conf = SparkConf() \
        .setAppName('Cube Build Beta') \
        .setMaster(master) \
        .set("spark.hadoop.validateOutputSpecs", "false") #TODO(xial): TEST PURPOSE, WILL BE REMOVED
    sc = SparkContext(conf=conf)

    # Read data into Spark
    data = sc.textFile(input_path)

    logger.info('Start building benchmark cube...')

    # Execute building cube
    exec_build(data, config)

    logging.info('Cube built successfully!')


if __name__ == '__main__':
    main()
