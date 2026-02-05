from typing import Callable

import pandas as pd
import numpy as np
from collections import defaultdict
from rapidfuzz import fuzz
import os
import pickle
import json
from pathlib import Path

CHUNK_SIZE = 1000

def build_ngram_index(map_file_path:str,
                      n:int = 3) -> tuple[dict,pd.Series]:
    cache_file = 'tmp_map.bin'
    index_to_name = pd.read_stata(map_file_path, columns=['index', 'std_name'], index_col='index')['std_name']
    index_to_name.index=index_to_name.index.astype('int')

    if os.path.exists(cache_file):
        with open(cache_file, 'rb') as f:
            ngram_index = pickle.load(f)
        return ngram_index, index_to_name

    # 构建 n-gram 倒排索引
    ngram_index = defaultdict(set)

    for index,name in index_to_name.items():
        # 生成 n-gram 并构建索引
        if len(name) >= n:
            for i in range(len(name) - n + 1):
                ngram_index[name[i:i + n]].add(index)
    print(f"倒排索引构建完成，包含 {len(ngram_index)} 个 n-gram 键")

    # 保存到缓存文件
    with open(cache_file, 'wb') as f:
        pickle.dump(ngram_index, f)

    return ngram_index, index_to_name

def get_candidates_by_ngram(query_name:str,
                            ngram_index:dict,
                            n:int = 3,
                            min_overlap:int = 5) -> set:
    if len(query_name) < n:
        return set()

    query_grams = set()
    for i in range(len(query_name) - n + 1):
        query_grams.add(query_name[i:i + n])

    candidate_indexs = set()
    overlap_count = defaultdict(int)

    for gram in query_grams:
        if gram in ngram_index:
            for index in ngram_index[gram]:
                overlap_count[index] += 1

    # 只保留重叠 n-gram 数 >= min_overlap 的候选
    for index, count in overlap_count.items():
        if count >= min_overlap:
            candidate_indexs.add(index)

    return candidate_indexs

def simple_match(df:pd.DataFrame,
                 ngram_index: dict,
                 name_to_index: pd.Series,
                 index_to_name: pd.Series,
                 algorithm:Callable=fuzz.token_sort_ratio,
                 name_col:str = "name_std",
                 threshold:float = 90.0):
    def _match_row(row):
        name = row[name_col]

        # (a) 精确匹配
        if name in name_to_index:
            row["match_type"] = 0
            row["result"] = name_to_index[name]
            return row

        # (b) 模糊匹配
        candidate_indexs = get_candidates_by_ngram(name, ngram_index)
        if not candidate_indexs:
            row["match_type"] = 3
            return row

        best_match_idx = None
        best_similarity = -1.0
        matches_above_threshold = {}
        for index in candidate_indexs:
            std_name=index_to_name[index]
            similarity = round(algorithm(name, std_name), 2)

            if similarity > threshold:
                matches_above_threshold[index] = similarity

            if similarity > best_similarity:
                best_similarity = similarity
                best_match_idx = index

        if matches_above_threshold:
            row["match_type"] = 1
            row["result"] = json.dumps(matches_above_threshold)
        else:
            row["match_type"] = 2
            row["result"] = json.dumps({best_match_idx: best_similarity})

        return row

    df[["match_type","result"]]=np.nan
    df=df.apply(_match_row, axis=1)
    return df

def match_file(file_path:str,
               output_path:str,
               map_file_path:str,
               id_col:str,
               origi_name_col:str,
               algorithm:Callable=fuzz.token_set_ratio,
               std_name_col:str="name_std"):
    ngram_index,index_to_name = build_ngram_index(map_file_path, n=3)
    name_to_index=pd.Series(index_to_name.index.values,index_to_name.values)

    reader = pd.read_csv(file_path, chunksize=CHUNK_SIZE, iterator=True, dtype=str, usecols=[id_col,origi_name_col,std_name_col])
    total_rows, filtered_rows = 0, 0
    first = True
    print("match_file: 开始处理文件：" + file_path)
    for i, chunk in enumerate(reader):
        total_rows += len(chunk)
        chunk = simple_match(chunk, ngram_index, name_to_index, index_to_name,algorithm)
        filtered_rows += len(chunk)
        chunk.to_csv(
            output_path,
            mode='w' if first else 'a',
            header=first,
            index=False
        )
        first = False
        print(f"已处理批次 {i + 1}, 累计行数: {total_rows:,}, 累计输出行数: {filtered_rows:,}")

def match_main(file_path,type,map_path,output_path,tmp_path=None,algorithm=fuzz.token_sort_ratio):
    if tmp_path is None:
        tmp_path = Path(__file__).parent / "temp" / "tmp_map.bin"

    if type=="OWNER":
        match_file(
            file_path,
            output_path,
            map_path,
            "serial_no",
            "own_name",
            algorithm=algorithm
        )
    elif type=="ASSIGNOR":
        match_file(
            file_path,
            output_path,
            map_path,
            "rf_id",
            "or_name",
            algorithm=algorithm
        )
    elif type=="ASSIGNEE":
        match_file(
            file_path,
            output_path,
            map_path,
            "rf_id",
            "ee_name",
            algorithm=algorithm
        )
    else:
        raise ValueError("unknown type: " + type)
