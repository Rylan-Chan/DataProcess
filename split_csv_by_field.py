import pandas as pd
import os

input_folder = r"D:\Projects\PythonProject\lbu_pst\Download\VAT异常结算导数（LM6049、DT3524）"
output_folder = r"D:\Documents\Desktop\output"
os.makedirs(output_folder, exist_ok=True)

# 读取所有csv文件放入列表
csv_files = [f for f in os.listdir(input_folder) if f.endswith(".csv")]

# 读取所有csv至Dataframe
df_list = [pd.read_csv(os.path.join(input_folder,csv_file),low_memory=False) for csv_file in csv_files]

# 将df合并
df = pd.concat(df_list)

print(df.shape)