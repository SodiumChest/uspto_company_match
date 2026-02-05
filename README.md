### 命令功能：

`standarlize <file_type> <file_path> <output_path>` 标准化公司名

`link <ciq_path> <linking_path> <output_path>` 链接ciq和gvkey

`merge <output_path> <type_0> <file_path_0> <type_1> <file_path_1> ...` 生成公司名：ID映射表

`match <source_path> <type> <map_path> <output_path> [temp_path]` 匹配公司名和ID

`check <file_path> <type> <map_path>` 人工检查match结果

`export <file_path> <type> <output_path> <map_path>` 将check后的结果导出

### 操作顺序：
1. 使用`standarlize`清洗各文件数据，得到`cleaned_*.csv`。其中，USPTO数据约24,000,000条。


2. 使用`link`链接`cleaned_firmname_ciq_subsidiary.csv`的`companyid`和`ultimateparentid`到`gvkey`，如果两者都没有匹配值则保留`companyid`，如果两者都有匹配值则全部保留（实际应用中发现，绝大多数情况都是两者都没有匹配值）。


3. 使用`merge`将`CRSP`、`COMPUSTAT`、`WRDS`、`CIQ`整合为一个文件`map.dta`（实际应用中，`CIQ`与前三者关联性不强，考虑硬件性能限制可以将其与前三者分开整合与匹配）。


4. 使用`match`匹配公司名和ID，导出初步匹配结果`matched_*.csv`。首先使用基于`n-gram`的倒排索引（`n=3, min_overlap=5`）挑出候选项，再使用`rapidfuzz.fuzz.token_sort_ratio`函数计算相似度（作为备选，可以使用`rapidfuzz.distance.JaroWinkler.similarity`函数），由于数据量很大，这一步耗时非常长。


5. 使用`check`对模糊匹配的结果进行人工检查，最终要保证所有结果均检查过。


6. 如果所有结果均检查过，使用`export`导出最终结果。

### 枚举值：

#### `match_type`

| 值 | 含义             |
|---|----------------|
| 0 | 精确匹配           |
| 1 | 模糊匹配，且置信度>=90% |
| 2 | 模糊匹配，置信度<90%   |
| 3 | 无匹配结果          |