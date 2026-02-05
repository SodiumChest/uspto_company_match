import pandas as pd
import numpy as np

CHUNK_SIZE=100000

def merge_dataframes(result:pd.DataFrame,
                     df:pd.DataFrame,
                     std_name_col:str,
                     origi_name_col:str,
                     id_value_col: str,
                     data_source: str,
                     id_type_col:str|None=None):
    df = pd.DataFrame({"std_name": df[std_name_col],
                       "origi_name": df[origi_name_col],
                       "id_type": id_value_col if id_type_col is None else df[id_type_col],
                       "id_value": df[id_value_col],
                       "data_source": data_source})
    for col in df.columns:
        if col not in result.columns:
            result[col] = np.nan
    for col in result.columns:
        if col not in df.columns:
            df[col] = np.nan

    result = pd.concat([result, df], ignore_index=True)
    #result = result.groupby('name', as_index=False).agg(lambda x: np.nan if (y:=x.dropna()).empty else y.iloc[0])
    result.drop_duplicates(subset=["std_name"],inplace=True,ignore_index=True)

    return result

def merge_file(result:pd.DataFrame,
               file_path:str,
               std_name_col: str,
               origi_name_col: str,
               id_value_col: str,
               data_source: str,
               id_type_col: str | None=None):
    reader = pd.read_csv(file_path, chunksize=CHUNK_SIZE, iterator=True, dtype=str)
    total_rows, filtered_rows = 0, 0
    print("开始处理文件："+file_path)
    for i, chunk in enumerate(reader):
        total_rows += len(chunk)
        result = merge_dataframes(result, chunk, std_name_col, origi_name_col, id_value_col, data_source, id_type_col)
        filtered_rows = len(result)
        print(f"已处理批次 {i + 1}, 累计行数: {total_rows:,}, 累计输出行数: {filtered_rows:,}")

    return result

def merge_main(args):
    result = pd.DataFrame()

    for i in range(1,len(args),2):
        type=args[i]
        if type=="CRSP":
            result = merge_file(
                result,
                args[i+1],
                "name_std",
                "comnam",
                "permno",
                "CRSP"
            )
        elif type=="COMPUSTAT":
            result = merge_file(
                result,
                args[i+1],
                "name_std",
                "conm",
                "gvkey",
                "COMPUSTAT"
            )
        elif type=="WRDS":
            result = merge_file(
                result,
                args[i+1],
                "name_std",
                "clean_company",
                "gvkey",
                "WRDS"
            )
        elif type=="CIQ":
            result=merge_file(
                result,
                args[i+1],
                "name_std",
                "companyname",
                "matched_key",
                "CIQ",
                "type"
            )
        else:
            raise ValueError("unknown type: "+type)

    result.to_stata(args[0],version=119,write_index=True)