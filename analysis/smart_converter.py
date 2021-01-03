import pandas as pd
import numpy as np


class SMARTConverter:
    def __init__(self, df):
        self.df = df
        self.df['failure'] = np.where(self.df['failure_time'].isnull(), 0, 1)
        self.df['failure_time'] = pd.to_datetime(self.df['failure_time'])
        self.df = self.df.sort_values(by=['failure_time'])
        self.combine_attributes()

    def combine_attributes(self):
        self.df['r_program'] = self.df.r_171.combine_first(self.df.r_181)
        self.df['r_erase'] = self.df.r_172.combine_first(self.df.r_182)
        self.df['n_program'] = self.df.n_171.combine_first(self.df.n_181)
        self.df['n_erase'] = self.df.n_172.combine_first(self.df.n_182)

        self.df['r_blocks'] = self.df.r_170.combine_first(self.df.r_180)
        self.df['n_blocks'] = self.df.n_170.combine_first(self.df.n_180)

        self.df['n_wearout'] = self.df.n_173.combine_first(
            self.df.n_177).combine_first(self.df.n_233)
        self.df = self.df.drop([
            'r_171', 'n_171', 'r_172', 'n_172', 'r_181', 'n_181', 'r_182',
            'n_182', 'r_180', 'n_180', 'n_173', 'n_177', 'n_233'
        ], axis=1)

