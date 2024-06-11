class DfOperator:
    def __int__(self, soure_df):
        self.source_df = soure_df

    def split_df_by_field(self, field):
        group_df = self.source_df.grouby()
