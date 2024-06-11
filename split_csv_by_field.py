import pandas as pd
import os
# from df_operator import

input_folder = r"D:\Projects\PythonProject\lbu_pst\Download\VAT异常结算导数（LM6049、DT3524）"
output_folder = r"D:\Documents\Desktop\output"
os.makedirs(output_folder, exist_ok=True)

# 读取所有csv文件放入列表
csv_files = [f for f in os.listdir(input_folder) if f.endswith(".csv")]

# 提取表头
columns = pd.read_csv(os.path.join(input_folder,csv_files[0]),low_memory=False).columns.tolist()

# 初始化Dataframe，放入表头
df = pd.DataFrame(columns = columns)

# 读取所有csv至Dataframe
df_list = [pd.read_csv(os.path.join(input_folder,csv_file),header=None,low_memory=False) for csv_file in csv_files]


# 将df合并
df = pd.concat(df_list)

group_df = df.grouby('server_code')

print(group_df)




def split_csv_by_server_code(input_folder, output_folder):
    # 创建输出文件夹
    os.makedirs(output_folder, exist_ok=True)

    # 获取输入文件夹下的所有 CSV 文件
    csv_files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]

    # 读取第一个 CSV 文件，提取表头
    first_csv = pd.read_csv(os.path.join(input_folder, csv_files[0]))
    header = first_csv.columns.tolist()

    # 初始化合并后的 DataFrame
    df = pd.DataFrame(columns=header)

    # 读取并合并所有 CSV 文件，忽略每个文件的表头
    for csv_file in csv_files:
        df = pd.concat([df, pd.read_csv(os.path.join(input_folder, csv_file), header=None)], ignore_index=True)

    # 根据 server_code 拆分数据
    grouped_data = df.groupby(header[0])  # 使用第一个字段（server_code）作为分组字段

    # 将每个组保存到单独的 CSV 文件
    for server_code, group in grouped_data:
        output_file = os.path.join(output_folder, f'{server_code}.csv')
        group.to_csv(output_file, index=False, header=header)  # 写入时保留表头


if __name__ == "__main__":
    # 输入文件夹路径（包含三个表结构相同的 CSV 文件）
    input_folder = '/path/to/input/folder'

    # 输出文件夹路径（保存按 server_code 拆分后的 CSV 文件）
    output_folder = '/path/to/output/folder'

    split_csv_by_server_code(input_folder, output_folder)
