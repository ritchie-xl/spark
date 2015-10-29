import argparse

all_states = ['AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL',
            'IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE',
            'NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD',
            'TN','TX','UT','VT','VA','WA','WV','WI','WY']

# Read the combo definition file
def read_combo_file(file):
    ret_val = [i.strip()[2:] for i in open(file,'r').readlines()[1:] if i[0] == '1']
    return ret_val

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
def build_parse_arguments():
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

