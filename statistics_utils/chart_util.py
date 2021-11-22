import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


class ChartUtil:
    @staticmethod
    def gen_histogram(data_list, n=0, x=list(), y='count', size_w=16, size_h=8):
        """
        Generate histogram chart
        :param data_list: dataframe
        :param n: number of data
        :param x: x name
        :param y: y name
        :param size_w: width
        :param size_h: height
        :return:
        """
        n = len(data_list) if n == 0 else n
        col = int(n/2+1) if (n%2) else int(n/2)
        fig, axes = plt.subplots(nrows=2, ncols=col)
        fig.set_size_inches(size_w, size_h)

        pd_df = data_list.toPandas()
        for i in range(len(x)):
            sns.countplot(data=pd_df, x=x[i], ax=axes[i%2][int(i/2)])
            plt.show()
