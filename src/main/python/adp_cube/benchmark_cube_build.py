from pyspark import SparkConf,SparkContext
import argparse

all_states = ['AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL',
            'IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE',
            'NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD',
            'TN','TX','UT','VT','VA','WA','WV','WI','WY']

# Read the combo definition file
def read_combo_file(file):
    return [i.strip()[2:] for i in open(file,'r').readlines()[1:] if i[0] == '1']


# Get all 12 months within the input quarter
def get_12_months(qtr):
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
    fields = line.split(",")
    combos = combo.strip().split(",")
    combo_num = combos[0]
    f_list = list()
    f_list.append(combo_num)
    for i in range(1, len(fields)):
        if combos[i] != '0':
            f_list.append(fields[i])
        else:
            f_list.append("")
    key = ",".join(f_list)
    return key


    #TODO: Add -t, --table for hive table
    #TODO: Add -p, --partition for partitions
def parse_arguments():
    """
    This function will build a argparsor for the main function,
    following parameters can be specified in this functions:
     -c, --combo [File/Path]: The combo definition file
     -i, --input [File/Path]: The input file path
     -y, --yyyymm [201506]: The year and month for this cube, retrospect 12 months
     -o, --output [File/Path]: The output file path
     -m, --metric [min,max,avg,count]: The matric will compute by the build function
     -s, --spark [local/Spark UI]: The spark master URL
    :return: argparsor
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--combo', help = 'The combo you want to compute')
    parser.add_argument('-i', '--input', help = 'The input file path')
    parser.add_argument('-q','--quarter', help = 'The year and month of the build')
    parser.add_argument('-o', '--output', help = 'Output Path')
    parser.add_argument('-m','--matric', help = 'The matrics you want to compute, separated by "," ')
    parser.add_argument('-sm','--master', help = 'The spark master url')
    parser.add_argument('-t','--target', help = 'The target column name to compute matrics')

    args = vars(parser.parse_args())
    output = args['output']
    yyyymm = args['quarter']
    args['output'] = output + '/' + yyyymm + '/' if output[-1] != '/' else output + yyyymm + '/'
    return args


def calculate_min(data, combo):
    return data \
        .map(lambda (key,value) : (build_key(combo,key), value)) \
        .reduceByKey(lambda a,b : a if a < b else b)


def calculate_max(data, combo):
    return data \
        .map(lambda (key,value) : (build_key(combo,key), value)) \
        .reduceByKey(lambda a,b : a if a > b else b)


def calculate_avg_with_cnt(data, combo):
    return data \
        .map(lambda (key,value):(build_key(combo,key),value)) \
        .combineByKey(lambda value: (value, 1),
                    lambda x, value: (x[0] + value, x[1] + 1),
                    lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .map(lambda (label, (value_sum, count)) :
                                    (label , str(count) + "," + str(value_sum/count)))


def calculate_sum(data, col_idx):
    return data\
        .map(lambda line: (",".join(line.strip().split(",")[:5]), float(line.strip().split(",")[col_idx])))\
        .reduceByKey(lambda a, b:a+b)


# TODO Design the way to apply filter to make it generic
def apply_filter(data, filter):
    return 0

class Calculator:
    def __init__(self, data = None, metrics = None, combo = None, index = None):
        self.data = data
        self.metrics = None
        self.combo = combo
        self.index = index

    def min(self):
        return self.data \
        .map(lambda (key,value) : (build_key(self.combo,key), value)) \
        .reduceByKey(lambda a,b : a if a < b else b)

    def max(self):
        return self.data \
        .map(lambda (key,value) : (build_key(self.combo,key), value)) \
        .reduceByKey(lambda a,b : a if a > b else b)

    def avg_with_cnt(self):
        return self.data \
        .map(lambda (key,value):(build_key(self.combo,key),value)) \
        .combineByKey(lambda value: (value, 1),
                    lambda x, value: (x[0] + value, x[1] + 1),
                    lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .map(lambda (label, (value_sum, count)) :
                                    (label , str(count) + "," + str(value_sum/count)))
    def sum(self):
        return self.data\
        .map(lambda line: (",".join(line.strip().split(",")[:5]), float(line.strip().split(",")[self.index])))\
        .reduceByKey(lambda a, b:a+b)

def exec_build(data, args):
    # Get all the arguments from the parser
    combo_file = args['combo']
    qtr = args['quarter']
    output = args['output']
    target = args['target']

        # Find all months within 12 months
    all_months = get_12_months(qtr)

    all_combos = read_combo_file(combo_file)
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
        .filter(lambda line: line.strip().split(",")[3] in all_states)
        #.filter(lambda line: line.strip().split(",")[?]) in ['T','A'])

    # Compute the total wage for each person within last 12 months
    person_total = calculate_sum(data, idx)

    # Iterate all combos
    for combo in all_combos:
        # Computer average
        # calculator = Calculator(data = data, combo = combo)
        # data_avg = calculator.avg_with_cnt()
        data_avg = calculate_avg_with_cnt(person_total, combo)

        # Apply filter employee count > 180
        data_new = data_avg.filter(lambda (x,y) : int(y.split(",")[0])>180)

        # Computer min
        # data_min = calculator.min()
        data_min = calculate_min(person_total, combo)

        # Computer max
        # data_max = calculator.max()
        data_max = calculate_max(person_total, combo)

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
    args = parse_arguments()
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
