import pandas as pd
import json
import numpy as np

def save(company_df:pd.DataFrame,
         match_df:pd.DataFrame,
         output_path:str):
    for i in match_df[match_df["matched_id"].notna()].iloc:
        company_df.at[i.name, "matched_id"]=i["matched_id"]
        company_df.at[i.name, "similarity"] = i["similarity"]

    company_df.to_csv(output_path, index=False)

def manual_check_main(file_path:str,
                      map_file_path:str,
                      origi_name_col:str,
                      std_name_col:str='name_std'):
    company_df = pd.read_csv(file_path, dtype=str)

    if 'matched_id' not in company_df.columns:
        company_df['matched_id'] = np.nan
    if 'similarity' not in company_df.columns:
        company_df['similarity'] = np.nan

    index_to_name = pd.read_stata(map_file_path, columns=['index', 'std_name'], index_col='index')['std_name']
    index_to_name.index = index_to_name.index.astype('int')

    match_df = company_df[(company_df['match_type'] == '1') | (company_df['match_type'] == '2')].copy()

    type_0 = company_df['match_type'] == '0'
    if type_0.any():
        company_df.loc[type_0, 'matched_id'] = company_df.loc[type_0, 'result']
        company_df.loc[type_0, 'similarity'] = 100.0

    type_3 = company_df['match_type'] == '3'
    if type_3.any():
        company_df.loc[type_0, 'matched_id'] = "-1"
        company_df.loc[type_3, 'similarity'] = 0.0

    save(company_df,match_df,file_path)

    cidx = 0
    while cidx < len(match_df):
        row = match_df.iloc[cidx]

        print("\033c", end='')
        print("输入0表示没有匹配项，其他数字表示匹配项")
        print("输入'a'向前一页，'d'向后一页，'j <idx>'跳转，'f'前一未标记项，'n'后一未标记项，'s'保存，'e'保存并退出\n")
        print(f"当前条目：{cidx + 1}/{len(match_df)}\n")
        print(f"公司名：{row[origi_name_col]}\n")
        print(f" 标准化公司名： {row[std_name_col]}\n")

        try:
            result_dict = json.loads(row['result'])
        except:
            result_dict = {}

        sorted_items = sorted(result_dict.items(), key=lambda x: x[1], reverse=True)
        chosen=row['matched_id']

        print("  i   similarity   name")
        if chosen=="0":
            print("> [  0]    0.00%")
        for i, (idx, sim) in enumerate(sorted_items):
            std_name = index_to_name.get(int(idx), f"Unknown Index: {idx}")
            if idx==chosen:
                print("> ",end='')
            print("[%3s]  %6s%%  "%(i+1,"%.2f"%sim)+std_name)
        if not sorted_items:
            print("没有匹配项")

        user_input = input("\n输入：").strip()
        if user_input=='':
            continue
        if user_input == '0':
            match_df.loc[row.name,'matched_id'] = '-1'
            match_df.loc[row.name,'similarity'] = 0.0
            cidx += 1
        elif user_input.isdigit():
            selected_idx = int(user_input)
            if 1 <= selected_idx <= len(sorted_items):
                match_df.loc[row.name,['matched_id','similarity']] = sorted_items[selected_idx-1]
                cidx += 1
        elif user_input == 'a':
            if cidx > 0:
                cidx -= 1
        elif user_input == 'd':
            if cidx < len(match_df) - 1:
                cidx += 1
        elif user_input[0] == 'j':
            jt=int(user_input.split()[1])
            if 1 <= jt <= len(match_df):
                cidx = jt-1
        elif user_input == 'f':
            while cidx>0 and pd.notna(match_df.iloc[cidx]['matched_id']):
                cidx -= 1
        elif user_input == 'n':
            while cidx < len(match_df)-1 and pd.notna(match_df.iloc[cidx]['matched_id']):
                cidx += 1
        elif user_input == 's':
            save(company_df,match_df,file_path)
        elif user_input == 'e':
            save(company_df,match_df,file_path)
            return
        else:
            continue
    save(company_df, match_df, file_path)

def post_process_companies(file_path:str,
                           map_file_path:str,
                           output_path:str,
                           id_col:str,
                           name_col:str):
    company_df = pd.read_csv(file_path, dtype=str)
    if company_df['matched_id'].isna().any():
        print("有项目未匹配")
        return

    map_df = pd.read_stata(map_file_path,index_col='index')

    company_df['matched_id']=company_df['matched_id'].astype(int)
    company_df.drop(columns=['result'], inplace=True)
    company_df.rename(columns={name_col:'original_name'}, inplace=True)
    company_df.rename(columns={'name_std': 'cleaned_name'}, inplace=True)
    company_df.rename(columns={'similarity': 'confidence_score'}, inplace=True)
    company_df['matched_name']=np.nan
    company_df['id_type']=np.nan
    company_df['data_source']=np.nan

    def _match_row(row):
        if pd.isna(row['matched_id']):
            return row
        matched_row=map_df.loc[row['matched_id']]
        row[["matched_id","matched_name","id_type","data_source"]]=matched_row[['id_value','origi_name',"id_type",'data_source']]
        return row
    mask=company_df['matched_id']==-1
    company_df.loc[mask,'matched_id']=np.nan
    company_df.loc[mask,'match_type']='3'
    company_df=company_df.apply(_match_row, axis=1)

    company_df.to_csv(output_path,index=False,columns=[
        id_col,"original_name","cleaned_name","id_type","matched_id","matched_name","data_source","match_type","confidence_score"
    ])
