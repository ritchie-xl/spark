import ConfigParser

file ='data/cube.properties'
parser = ConfigParser.RawConfigParser()
parser.read(file)

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
        combo_name = combo + " only"
    else:
        for field in combo:
            if field in all_dims:
                tmp[dim_idx[field]] = '1'
            combo_name = " and ".join(combo)
    combos[",".join(tmp)] = combo_name

print combos




