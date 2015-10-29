# TODO ADD MORE METRICS AND THE FUNCTIONS TO COMPUTE THE METRICS
class CubeMetric:
    """A object to save all the matrics and the functions to
        compute corresponding metrics

    Attributes:
    MAX: compute the maximum for the target variable
    MIN: compute the minimum for the target variable
    CNT: compute the count for the target variable
    SUM: compute the sum for the target variable
    AVG: compute the average for the target variable
    """

    def __init__(self,cube_min=1, cube_max = 1, cube_avg = 1, cube_cnt = 1, cube_sum = 1):
        self.MAX = cube_max
        self.MIN = cube_min
        self.CNT = cube_cnt
        self.AVG = cube_avg
        self.SUM = cube_sum

    def compute_min(self, data):
        return data.map(lambda line: line.strip())




