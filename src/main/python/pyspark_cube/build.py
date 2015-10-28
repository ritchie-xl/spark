import os
import argparse
import sys

os.environ['SPARK_HOME'] = '/Users/Lei/Documents/Spark-1.3.0'
sys.path.append('/Users/Lei/Documents/Spark-1.3.0/python')

try:
    from pyspark import SparkConf,SparkContext
except:
    print 'error'


# job, ind, state, rate_type
def read_combo_file(file):
    ret_val = [i.strip()[2:] for i in open(file,'r').readlines()[1:] if i[0] == '1']
    return ret_val


def get_12_months(yyyymm):
    ret_val = [yyyymm]
    yyyy = int(yyyymm[:4])
    mm = int(yyyymm[4:])

    for i in range(11):
        mm = mm - 1
        if mm == 0:
            yyyy -= 1
            mm = 12

        new_yyyymm = str(yyyy) + '0' + str(mm) if mm < 10 else str(yyyy) + str(mm)
        ret_val.append(new_yyyymm)

    return ret_val


def build_key(combo, line):
    fields = line.split(",")
    combos = combo.strip().split(",")
    combo_num = combos[0]
    f_list = []
    f_list.append(combo_num)
    for i in range(1, len(fields)):
        if combos[i] != '0':
            f_list.append(fields[i])
        else:
            f_list.append("")
    key = ",".join(f_list)

    return key


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--combo', help = 'Enter the combo you want to compute')
    parser.add_argument('-i', '--input', help = 'Enter the input file path')
    parser.add_argument('-m','--yyyymm', help = 'Enter the year and month of the build')
    parser.add_argument('-o', '--output', help = 'Enter the output folder name')

    args = vars(parser.parse_args())

    combo_file = args['combo']
    data_file = args['input']
    yyyymm = args['yyyymm']
    output = args['output']

    all_months = get_12_months(yyyymm)

    all_combos = read_combo_file(combo_file)

    conf = SparkConf().setAppName('Cube Build Beta').setMaster('local[4]')
    sc = SparkContext(conf=conf)

    data = sc.textFile(data_file)

    header = data.first()

    # Get all months's data within last 12 months
    data = data.filter(lambda x : x != header).filter(lambda line: line.strip().split(",")[-1] in all_months)

    # Compute the total wage for each person within last 12 months
    person_sum = data\
                .map(lambda line: (",".join(line.strip().split(",")[:5]), float(line.strip().split(",")[5])))\
                .reduceByKey(lambda a, b:a+b)

    # Iterate all combos
    for combo in all_combos:
        # Computer average
        data_avg = person_sum \
                    .map(lambda (key,value):(build_key(combo,key),value)) \
                    .combineByKey(lambda value: (value, 1),
                                lambda x, value: (x[0] + value, x[1] + 1),
                                lambda x, y: (x[0] + y[0], x[1] + y[1])) \
                    .map(lambda (label, (value_sum, count)) : (label , str(count) + "," + str(value_sum/count)))

        # Apply filter employee count > 180
        data_new = data_avg.filter(lambda (x,y) : int(y.split(",")[0])>180)

        # Computer min
        data_min = person_sum \
                    .map(lambda (key,value) : (build_key(combo,key), value)) \
                    .reduceByKey(lambda a,b : a if a < b else b)

        # Computer max
        data_max = person_sum \
                    .map(lambda (key,value) : (build_key(combo,key), value)) \
                    .reduceByKey(lambda a,b : a if a > b else b)

        data_final = data_new.join(data_min).join(data_max) \
                    .map(lambda (key, value) : ",".join(key.split(",") + \
                                                        [i.strip("'()") for i in str(value).split(",")])) \
                    .repartition(1)

        for i in data_final.collect():
            print i
        data_final.saveAsTextFile(combo.split(",")[0])




if __name__ == '__main__':
    main()